"""Tests for the description each dataset column gets, and the one spelling of each transport.

The descriptions are read from merged schemas, the process's catalog's and synthetic ones, since
the merge is what applies the rules to every column.
"""

import pytest

import deephaven_massive.namespaces._crypto as crypto
import deephaven_massive.namespaces._forex as forex
import deephaven_massive.namespaces._futures as futures
import deephaven_massive.namespaces._indicators as indicators
import deephaven_massive.namespaces._indices as indices
import deephaven_massive.namespaces._stocks as stocks
from deephaven_massive._datasets.catalog import Catalog, DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._datasets.column_descriptions import JSON_TEXT_NOTE, TRANSPORT_LABELS
from deephaven_massive._datasets.schema import ColumnType, dataset_schema
from deephaven_massive._enums import AssetClass, ClassFromPrefix, NotBars
from deephaven_massive._massive import Field, Source, Transport, WireType, WireUnit, source
from deephaven_massive._session import CATALOG

R, W, F = Transport.REST, Transport.WS, Transport.FLAT_FILES


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


def lookup(transport: Transport, key: str) -> Source:
    return source(transport, key)


def described(text: str, wire_key: str, column: str) -> Field:
    return Field(wire_key=wire_key, column=column, wire_type=WireType.DOUBLE, description=text)


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
        TRANSPORT_LABELS[Transport.WS] = "WS"  # type: ignore[index]  # the point: read-only


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
        f"The row's instrument, as Massive's ticker. From {'; '.join(symbol)}"
    )
    assert dict(columns["Symbol"].wire_keys) == {R: (None,), F: ("ticker",), W: ("sym",)}
    assert columns["Symbol"].supplied_by == (R, F, W)
    # REST and the flat files describe `sip_timestamp` alike, so they share one item.
    assert (
        wire(R, history, "SipTimestamp").description == wire(F, files, "SipTimestamp").description
    )
    timestamp = [
        f'REST and flat files `sip_timestamp`: "{wire(R, history, "SipTimestamp").description}"',
        f'WebSocket `t`: "{wire(W, "stocks/T", "SipTimestamp").description}"',
    ]
    assert columns["Timestamp"].description == (
        f"The row's event time. From {'; '.join(timestamp)}"
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
        "The instrument Massive lists the row under, as Massive's ticker. From "
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
        "The row's instrument, as Massive's ticker. From REST `sym`."
    )


def test_a_key_part_quoted_differently_by_one_transport_names_each_source() -> None:
    # A custom bar's `t` is its window's start, a grouped daily bar's its end: each quote
    # names its path, and paths that quote one text share an item.
    def stamp(text: str) -> Field:
        return Field(
            wire_key="t", column="T", wire_type=WireType.EPOCH, unit=WireUnit.MS, description=text
        )

    texts = {"/first": "The start.", "/second": "The end.", "/third": "The start."}
    paths = tuple(texts)
    dataset = DatasetSpec(
        name="reference.x",
        summary="A dataset for the tests.",
        fetch_path=paths[0],
        extra_paths=paths[1:],
        keys={(R, path): SourceKeys(KeySpec("Ticker"), KeySpec("T")) for path in paths},
    )
    ticker = Field(wire_key="sym", column="Ticker", wire_type=WireType.STRING)
    made = {
        path: Source(key=path, transport=R, fields=(ticker, stamp(text)))
        for path, text in texts.items()
    }
    columns = dataset_schema(dataset, (dataset,), lambda _, key: made[key]).columns
    timestamp = next(c for c in columns if c.name == "Timestamp")
    assert timestamp.description == (
        "The row's event time. From REST `/first` and REST `/third` `t`: "
        '"The start."; REST `/second` `t`: "The end."'
    )
    # The symbol's key is quoted alike by every path, so its item names none.
    symbol = next(c for c in columns if c.name == "Symbol")
    assert symbol.description.endswith("From REST `sym`.")


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
        (
            indices.BARS,
            "Chicago time (01:00 New York time)",
            "a week's Sunday, a month's, quarter's, or year's 1st",
        ),
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
    assert (
        "Every other stamp, as each source sends it (quoted next), is moved there. From REST"
        in (described)
    )
    # A bar at a stamp Massive is not known to use is withheld, said before the sources' stamps.
    assert "Massive is not known to use is withheld, with a WARNING" in described


def test_a_futures_bar_keeps_massive_s_window_start_and_its_session() -> None:
    assert description(futures.BARS, "TimestampStart").startswith(
        "The start of the bar's window, as an Instant: Massive's own window start. From REST "
        "`window_start`"
    )
    date_text = description(futures.BARS, "Date")
    assert date_text.startswith("The futures session of the bar (its trading date)")
    assert "From REST `session_end_date`" in date_text
    end = description(futures.BARS, "TimestampEnd")
    assert "17:00 Chicago time on its label, Massive's `session_end_date`, which is its last" in end
    assert "only once no later session can still join it" in end
    assert "An intraday bar ends at Massive's `e` where the WebSocket sends one" in end
    stocks_end = description(stocks.BARS, "TimestampEnd")
    assert "An intraday bar ends at Massive's `e` where the WebSocket sends one" in stocks_end
    trades = description(futures.TRADES, "Date")
    assert "the session Massive's futures schedules place it in" in trades
    assert "is never published without a date" in trades
    assert "`unlisted_contracts` and `unscheduled_products`" in trades


def test_the_library_describes_its_time_columns() -> None:
    stock_end = description(stocks.BARS, "TimestampEnd")
    assert stock_end.startswith("The exclusive end of the bar's window, as an Instant")
    assert 'on=["Symbol", "Timestamp>=TimestampEnd"]' in stock_end
    assert "at 00:00 New York time after its window's last day. A row read before" in stock_end
    index_end = description(indices.BARS, "TimestampEnd")
    assert (
        "ends at 00:00 Chicago time (01:00 New York time) after its window's last day. A row"
        in (index_end)
    )
    index_start = description(indices.BARS, "TimestampStart")
    assert index_start.startswith(
        "The start of the bar's window, as an Instant. A bar of a day or longer starts at 00:00 "
        "Chicago time (01:00 New York time) of its window's first day (a week's Sunday, a "
        "month's, quarter's, or year's 1st), Massive's own stamp. Massive's index days are "
        "Chicago days"
    )
    assert "though Massive's documentation says index days are Eastern" in index_start
    assert description(stocks.BARS, "Date").startswith(
        "The trading day of the bar, as ISO yyyy-MM-dd text: the New York date of `TimestampStart`"
    )
    assert "join longer bars on `TimestampEnd`" in description(stocks.BARS, "Date")
    assert description(crypto.TRADES, "Date") == (
        "The trading day of the row, as ISO yyyy-MM-dd text: the UTC date of `Timestamp`."
    )
    insert = description(stocks.SNAPSHOTS, "TimestampInsert")
    assert insert.startswith("When the library received the row, by the local clock.")


def test_the_arrival_time_names_only_what_the_dataset_has() -> None:
    # A REST list: REST pages and polls, no history, flat files, or WebSocket.
    assert description(stocks.SNAPSHOTS, "TimestampInsert") == (
        "When the library received the row, by the local clock. Rows that arrive together share "
        "it. It isn't Massive's time, and later rows can carry earlier times. In a `poll()` "
        "table, it's the time of the poll the row's values came from."
    )
    # A stream with history from REST and flat files, and no poll.
    assert description(crypto.TRADES, "TimestampInsert") == (
        "When the library received the row, by the local clock. Rows that arrive together share "
        "it. It isn't Massive's time, and later rows can carry earlier times. On `history()` "
        "rows, it's the download time."
    )


def test_the_daily_summaries_and_indicators_describe_their_windows() -> None:
    assert description(stocks.DAILY_OPEN_CLOSE, "TimestampStart") == (
        "The start of the day the summary covers, as an Instant: 00:00 New York time of `Date`."
    )
    assert description(stocks.DAILY_OPEN_CLOSE, "TimestampEnd") == (
        "The exclusive end of the day the summary covers, as an Instant: 00:00 New York time of "
        f"the next day. {FINISHED}"
    )
    assert description(indices.DAILY_OPEN_CLOSE, "TimestampEnd").endswith(
        f"says index days are Eastern. {FINISHED}"
    )
    # A crypto summary not in UTC gets its date's widest window in any zone.
    for name in ("TimestampStart", "TimestampEnd"):
        assert "from 14 hours before 00:00 UTC of `Date` to 12 hours after" in description(
            crypto.DAILY_OPEN_CLOSE, name
        )
    assert description(crypto.DAILY_OPEN_CLOSE, "Date").startswith(
        "The day the summary covers, as ISO yyyy-MM-dd text: a UTC calendar day. From REST `day`"
    )
    assert description(indicators.SMA, "TimestampStart").startswith(
        "The start of the bar the value is computed at, as an Instant."
    )
    assert "day zone of the symbol's class" in description(indicators.SMA, "TimestampEnd")
    assert "for an index at 00:00 Chicago time" in description(indicators.SMA, "TimestampStart")
    assert "day zone of the symbol's class" in description(indicators.SMA, "Date")


FINISHED = (
    "A row read before its window ended has `TimestampInsert` before `TimestampEnd`, and its "
    'values can still change; `t.where("TimestampEnd <= TimestampInsert")` keeps only the rows '
    "read after their window ended. That test reads the local clock, which can be off, and a "
    "row read within moments of its end, or from a delayed feed, may still lack the window's "
    "last trades; an end set later than the window's (an index day summary's extra hour) only "
    "keeps a finished row out longer."
)


@pytest.mark.parametrize(
    "dataset",
    [stocks.BARS, futures.BARS, crypto.DAILY_OPEN_CLOSE, indicators.SMA, indicators.MACD],
    ids=lambda spec: spec.name,
)
def test_every_end_says_how_to_keep_only_finished_rows(dataset: DatasetSpec) -> None:
    # Every dataset with `TimestampEnd` says the same thing in the same words.
    assert FINISHED in description(dataset, "TimestampEnd")


def test_a_futures_end_says_a_longer_bar_can_still_grow() -> None:
    assert "though later sessions can still join it" in description(futures.BARS, "TimestampEnd")


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
    assert columns["TimestampInsert"].description.endswith(
        "the poll the row's values came from. Another."
    )
    bare = Source(key="/notes", transport=R, fields=(described("", "p", "Price"),))
    alone = dataset_schema(dataset, (dataset,), lambda *_: bare).columns[0]
    assert alone.description == "A note."
    # A vendor text without its closing period still ends its sentence before the note.
    open_ended = Source(key="/notes", transport=R, fields=(described("A price", "p", "Price"),))
    closed = dataset_schema(dataset, (dataset,), lambda *_: open_ended).columns[0]
    assert closed.description == "A price. A note."
    assert "Split-adjusted when" in description(stocks.BARS, "Close")
    assert "multiplied by its ratio" in description(stocks.BARS, "Volume")
