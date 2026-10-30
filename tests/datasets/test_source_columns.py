"""Tests for the key rename of one source for one dataset, its required keys, and widening."""

import dataclasses
from typing import cast

import pyarrow as pa
import pytest

from deephaven_massive._datasets.catalog import Catalog, DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._datasets.row_times import RowTimes
from deephaven_massive._datasets.source_columns import (
    KEY_COLUMNS,
    SourceColumn,
    key_column,
    rename,
    required_fields,
    widen,
)
from deephaven_massive._datasets.timestamps import TimeUnit
from deephaven_massive._enums import AssetClass, NotBars, Timespan
from deephaven_massive._massive import Field, Source, Transport, WireType, WireUnit, source
from deephaven_massive._namespaces import stocks

R, W, F = Transport.REST, Transport.WS, Transport.FLAT_FILES

# The wire interface names `Source.memberships` but exports no constructor for an element.
MEMBERSHIP = type(source(W, "stocks/T").memberships[0])


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
