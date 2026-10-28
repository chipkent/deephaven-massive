"""Tests for the column types, `Schema`, the key rename, and the merge of a kind's sources."""

import dataclasses
from datetime import UTC, date, datetime

import pyarrow as pa
import pytest
from deephaven import arrow as dh_arrow

from deephaven_massive._datasets.catalog import Catalog, DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._datasets.schema import (
    COLUMN_TYPES,
    KEY_COLUMNS,
    Column,
    ColumnType,
    Schema,
    SourceColumn,
    dataset_schema,
    join,
    key_column,
    pascal_case,
    rename,
    required_fields,
    widen,
)
from deephaven_massive._datasets.timestamps import TimeUnit
from deephaven_massive._enums import AssetClass
from deephaven_massive._errors import MassiveError
from deephaven_massive._massive import Field, Source, Transport, WireType, WireUnit, source
from deephaven_massive._tables.columns import col_defs

R, W, F = Transport.REST, Transport.WS, Transport.FLAT_FILES
ALL_TYPES = Schema("all", tuple(Column(t.name.title().replace("_", ""), t) for t in ColumnType))
# The wire interface names `Source.memberships` but exports no constructor for an element.
MEMBERSHIP = type(source(R, "/v3/snapshot/indices").memberships[0])


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
    assert ColumnType.INT_ARRAY.arrow_type == pa.list_(pa.int32())
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


def test_every_wire_type_has_a_column_type() -> None:
    assert set(COLUMN_TYPES) == set(WireType)
    assert COLUMN_TYPES[WireType.EPOCH] is COLUMN_TYPES[WireType.ISO_INSTANT] is ColumnType.INSTANT
    assert COLUMN_TYPES[WireType.JSON_TEXT] is ColumnType.STRING
    assert COLUMN_TYPES[WireType.BOOL] is ColumnType.BOOLEAN
    assert COLUMN_TYPES[WireType.DATE] is ColumnType.LOCAL_DATE
    assert COLUMN_TYPES[WireType.INT_ARRAY] is ColumnType.INT_ARRAY
    # A dataset column is never a 32-bit int: exchange and tape ids are long.
    assert ColumnType.INT not in COLUMN_TYPES.values()


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
    assert {c.name: c.data_type for c in table.columns} == col_defs(ALL_TYPES)
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
    with pytest.raises(MassiveError, match=r"schema mismatch in all\.IntArray.*7140\.08"):
        ALL_TYPES.to_arrow([{"IntArray": [7140.08]}])
    values = ALL_TYPES.to_arrow([{"IntArray": [1.0, None, 3]}]).column("IntArray")
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


def test_join_widens_only_by_the_lattice() -> None:
    long, double, ints = WireType.LONG, WireType.DOUBLE, WireType.INT_ARRAY
    assert join(long, double) is join(double, long) is WireType.DOUBLE
    assert join(long, ints) is join(ints, long) is WireType.INT_ARRAY
    assert join(WireType.EPOCH, WireType.EPOCH) is WireType.EPOCH
    for same in WireType:
        assert join(same, same) is same
    assert join(double, ints) is None
    assert join(WireType.EPOCH, WireType.ISO_INSTANT) is None
    assert join(WireType.STRING, WireType.JSON_TEXT) is None


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
        channels={None: key.split("/")[1]} if transport is W else {},
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


def test_the_session_key_keeps_its_column() -> None:
    files = (F, "us_futures_{venue}/trades_v1")
    futures = DatasetSpec(
        name="futures.trades",
        summary="A dataset for the tests.",
        asset_class=AssetClass.FUTURES,
        flat_file_prefixes={None: files[1]},
        keys={
            files: SourceKeys(
                KeySpec("Ticker"), KeySpec("Timestamp"), session=KeySpec("SessionEndDate")
            )
        },
    )
    names = [c.column for c in rename(futures, source(*files))]
    assert names[:2] == ["Symbol", "Timestamp"]
    assert "SessionEndDate" in names
    assert KEY_COLUMNS == {"symbol": "Symbol", "time": "Timestamp", "fanout": "Underlying"}


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
    snapshot = (R, "/v3/snapshot/indices")
    values = spec(
        "indices.values",
        {snapshot: SourceKeys(KeySpec("Ticker"), KeySpec("Timestamp"))},
        asset_class=AssetClass.INDICES,
        kind="values",
    )
    # The kind's Timestamp is the snapshot's own LastUpdated, which the wire client decodes.
    fields = required_fields(values, source(*snapshot))
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
        history_path=trades[1],
        flat_file_prefixes={None: files[1]},
        keys={
            trades: SourceKeys(
                KeySpec("ticker", from_request=True),
                KeySpec("Timestamp"),
                session=KeySpec("SessionEndDate"),
            ),
            files: SourceKeys(
                KeySpec("Ticker"), KeySpec("Timestamp"), session=KeySpec("SessionEndDate")
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
    assert key_column("symbol", KeySpec("Ticker")) == "Symbol"
    assert key_column("time", KeySpec("SipTimestamp")) == "Timestamp"
    assert key_column("fanout", KeySpec("UnderlyingTicker")) == "Underlying"
    assert key_column("fanout", KeySpec("FilerCik", instrument=False)) == "FilerCik"
    assert key_column("session", KeySpec("SessionEndDate")) == "SessionEndDate"


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
    assert names[:3] == ("Symbol", "AdditionalUnderlyings", "Cfi")
    assert schema.name == "options.contracts"
    # Only Symbol and Timestamp lead; Underlying keeps its field's place in the source.
    fields = [f.column for f in source(*contracts).fields if f.column != "Ticker"]
    assert names[1:] == tuple("Underlying" if c == "UnderlyingTicker" else c for c in fields)


def test_the_merge_follows_the_membership_positions() -> None:
    # Declared fetch, history, stream, files; the stocks trades group puts the history path
    # (position 0) before the last-trade path (position 1).
    sip = SourceKeys(KeySpec("Ticker"), KeySpec("SipTimestamp"))
    history = (R, "/v3/trades/{stockTicker}")
    last = (R, "/v2/last/trade/{stocksTicker}")
    trades = DatasetSpec(
        name="stocks.trades",
        summary="A dataset for the tests.",
        asset_class=AssetClass.STOCKS,
        kind="trades",
        fetch_path=last[1],
        history_path=history[1],
        channels={None: "T"},
        keys={
            last: sip,
            history: SourceKeys(KeySpec("stockTicker", from_request=True), sip.time),
            (W, "stocks/T"): sip,
        },
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

    names = dataset_schema(trades, (trades,), tables).column_names
    # The history path's columns come before the last-trade path's own Zeta.
    assert names.index("Conditions") < names.index("Zeta")
    assert names[-1] == "Zeta"


def test_the_merge_joins_types_across_sources() -> None:
    # The quote stream sends one condition code as an integer; REST sends a list.
    nbbo, stream = (R, "/v2/last/nbbo/{stocksTicker}"), (W, "stocks/Q")
    both = SourceKeys(KeySpec("Ticker"), KeySpec("SipTimestamp"))
    quotes = DatasetSpec(
        name="stocks.quotes",
        summary="A dataset for the tests.",
        asset_class=AssetClass.STOCKS,
        fetch_path=nbbo[1],
        history_path="/v3/quotes/{stockTicker}",
        channels={None: "Q"},
        keys={nbbo: both, stream: both},
    )
    schema = dataset_schema(quotes, (quotes,), lookup)
    types = {c.name: c.type for c in schema.columns}
    assert types["Conditions"] is ColumnType.INT_ARRAY
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
        channels={None: "T"},
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
        channels={None: "T"},
    )
    second = DatasetSpec(
        name="stocks.y",
        summary="A dataset for the tests.",
        asset_class=AssetClass.STOCKS,
        channels={None: "Q"},
    )
    tables = {"stocks/T": made, "stocks/Q": other}
    schema = dataset_schema(stream, (stream, second), lambda _, key: tables[key])
    assert [(c.name, c.type) for c in schema.columns] == [("At", ColumnType.INSTANT)]


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
        fetch_path="/first",
        history_path="/second",
        channels={None: "T"},
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
    for asset_class in (AssetClass.OPTIONS, AssetClass.STOCKS, None):
        dataset = DatasetSpec(
            name="reference.x",
            summary="A dataset for the tests.",
            asset_class=asset_class,
            fetch_path="/shared",
        )
        (column,) = dataset_schema(dataset, (dataset,), lambda *_: made).columns
        found[asset_class] = column.description
    assert found == {
        AssetClass.OPTIONS: "Per share of the underlying.",
        AssetClass.STOCKS: "Per whole share.",
        None: "Per whole share.",
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
        (column,) = dataset_schema(dataset, (dataset,), lambda *_: made).columns
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
    fetch_path="/v2/last/trade/{stocksTicker}",
    history_path="/v3/trades/{stockTicker}",
    channels={None: "T"},
    flat_file_prefixes={None: "us_stocks_sip/trades_v1"},
    keys={
        (R, "/v2/last/trade/{stocksTicker}"): TRADES_KEYS,
        (R, "/v3/trades/{stockTicker}"): SourceKeys(
            KeySpec("stockTicker", from_request=True), KeySpec("SipTimestamp")
        ),
        (W, "stocks/T"): TRADES_KEYS,
        (F, "us_stocks_sip/trades_v1"): TRADES_KEYS,
    },
)
CRYPTO_TRADES = DatasetSpec(
    name="crypto.trades",
    summary="Trades of crypto pairs, tick by tick.",
    asset_class=AssetClass.CRYPTO,
    kind="trades",
    history_path="/v3/trades/{cryptoTicker}",
    channels={None: "XT"},
    keys={
        (R, "/v3/trades/{cryptoTicker}"): SourceKeys(
            KeySpec("cryptoTicker", from_request=True), KeySpec("ParticipantTimestamp")
        ),
        (W, "crypto/XT"): SourceKeys(KeySpec("Pair"), KeySpec("ParticipantTimestamp")),
    },
)
TRADES = Catalog((STOCK_TRADES, CRYPTO_TRADES))


def wire(transport: Transport, key: str, column: str) -> Field:
    return source(transport, key).field(column)


def test_a_column_only_other_members_supply_names_them() -> None:
    columns = {c.name: c for c in TRADES.dataset_schema(CRYPTO_TRADES).columns}
    tape = columns["Tape"]
    assert tape.description == "Always null for `crypto.trades`; supplied by `stocks.trades`."
    assert (tape.supplied_by, dict(tape.wire_keys)) == ((), {})
    # Crypto renames its own participant time to Timestamp, so the stocks column is union-only.
    assert columns["ParticipantTimestamp"].description == (
        "Always null for `crypto.trades`; supplied by `stocks.trades`."
    )
    # Each dataset gets its own text, never the kind's: the stocks Tape is the vendor's.
    stocks_tape = next(c for c in TRADES.dataset_schema(STOCK_TRADES).columns if c.name == "Tape")
    assert stocks_tape.description == wire(R, "/v2/last/trade/{stocksTicker}", "Tape").description


def test_key_columns_quote_the_wire_key_or_the_request_parameter() -> None:
    columns = {c.name: c for c in TRADES.dataset_schema(STOCK_TRADES).columns}
    last, history = "/v2/last/trade/{stocksTicker}", "/v3/trades/{stockTicker}"
    files = "us_stocks_sip/trades_v1"
    symbol = [
        "The instrument, in the library's canonical symbol form.",
        f'REST `T`: "{wire(R, last, "Ticker").description}"',
        "REST: the request's `stockTicker` parameter.",
        f'FLAT_FILES `ticker`: "{wire(F, files, "Ticker").description}"',
        f'WS `sym`: "{wire(W, "stocks/T", "Ticker").description}"',
    ]
    assert columns["Symbol"].description == " ".join(symbol)
    assert dict(columns["Symbol"].wire_keys) == {R: ("T", None), F: ("ticker",), W: ("sym",)}
    assert columns["Symbol"].supplied_by == (R, F, W)
    timestamp = [
        "The row's primary time, as an Instant.",
        f'REST `t`: "{wire(R, last, "SipTimestamp").description}"',
        f'REST `sip_timestamp`: "{wire(R, history, "SipTimestamp").description}"',
        f'FLAT_FILES `sip_timestamp`: "{wire(F, files, "SipTimestamp").description}"',
        f'WS `t`: "{wire(W, "stocks/T", "SipTimestamp").description}"',
    ]
    assert columns["Timestamp"].description == " ".join(timestamp)


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
        "The underlying instrument, in the library's canonical symbol form. "
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
    (column,) = dataset_schema(status, (status,), lambda *_: made).columns
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
        "The instrument, in the library's canonical symbol form. REST `sym`."
    )
