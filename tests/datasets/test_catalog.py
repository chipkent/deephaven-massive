"""Tests for the dataset catalog: the specs, the merge cache, the catalog gates, and coverage.

The catalog gates run on the process's catalog and, with a negative test each, on synthetic
specs over the real generated sources. The coverage test proves that every generated source
is read by some dataset, or deferred with a reason and an issue in `DEFERRED`.
"""

import dataclasses
import re
import threading
from collections.abc import Callable, Iterable, Mapping
from concurrent.futures import ThreadPoolExecutor
from typing import assert_never

import pytest

from deephaven_massive._datasets.catalog import (
    NO_NAMED_TICKER,
    Catalog,
    DatasetSpec,
    KeySpec,
    SourceKeys,
    timespan_key,
)
from deephaven_massive._datasets.row_times import LIBRARY_COLUMNS, RowTimes
from deephaven_massive._datasets.schema import ColumnType
from deephaven_massive._enums import (
    AssetClass,
    ClassFromPrefix,
    IndexPrefix,
    NotBars,
    PollMode,
    Subscription,
    Timespan,
)
from deephaven_massive._errors import BrokenGuaranteeError
from deephaven_massive._massive import RestPath, Source, Transport, sources
from deephaven_massive._session import CATALOG

R, W, F = Transport.REST, Transport.WS, Transport.FLAT_FILES


def keys(
    symbol: str | KeySpec | None = None,
    time: str | KeySpec | None = None,
    *,
    fanout: str | KeySpec | None = None,
    date: str | KeySpec | None = None,
    end: str | KeySpec | None = None,
) -> SourceKeys:
    # A plain string names the source's own column; a KeySpec says more.
    def key(value: str | KeySpec | None) -> KeySpec | None:
        return KeySpec(value) if isinstance(value, str) else value

    return SourceKeys(key(symbol), key(time), end=key(end), fanout=key(fanout), date=key(date))


def request(parameter: str) -> KeySpec:
    return KeySpec(parameter, from_request=True)


STOCK_TRADES = DatasetSpec(
    name="stocks.trades",
    summary="A dataset for the tests.",
    asset_class=AssetClass.STOCKS,
    kind="trades",
    history_path="/v3/trades/{stockTicker}",
    channels={NotBars.ANY: "T"},
    flat_file_prefixes={NotBars.ANY: "us_stocks_sip/trades_v1"},
    keys={
        (R, "/v3/trades/{stockTicker}"): keys(request("stockTicker"), "SipTimestamp"),
        (W, "stocks/T"): keys("Ticker", "SipTimestamp"),
        (F, "us_stocks_sip/trades_v1"): keys("Ticker", "SipTimestamp"),
    },
    time_axis=("timestamp.gte", "timestamp.lt"),
)
# The one-row dataset that reads the rest of the trades/stocks group.
STOCK_LAST_TRADE = DatasetSpec(
    name="stocks.last_trade",
    summary="A dataset for the tests.",
    asset_class=AssetClass.STOCKS,
    kind="trades",
    fetch_path="/v2/last/trade/{stocksTicker}",
    keys={(R, "/v2/last/trade/{stocksTicker}"): keys("Ticker", "SipTimestamp")},
    revision_keys=("Timestamp",),
)
CRYPTO_TRADES = DatasetSpec(
    name="crypto.trades",
    summary="A dataset for the tests.",
    asset_class=AssetClass.CRYPTO,
    kind="trades",
    history_path="/v3/trades/{cryptoTicker}",
    channels={NotBars.ANY: "XT"},
    flat_file_prefixes={NotBars.ANY: "global_crypto/trades_v1"},
    keys={
        (R, "/v3/trades/{cryptoTicker}"): keys(request("cryptoTicker"), "ParticipantTimestamp"),
        (W, "crypto/XT"): keys("Pair", "ParticipantTimestamp"),
        (F, "global_crypto/trades_v1"): keys("Ticker", "ParticipantTimestamp"),
    },
    time_axis=("timestamp.gte", "timestamp.lt"),
)
CRYPTO_LAST_TRADE = DatasetSpec(
    name="crypto.last_trade",
    summary="A dataset for the tests.",
    asset_class=AssetClass.CRYPTO,
    kind="trades",
    fetch_path="/v1/last/crypto/{from}/{to}",
    keys={(R, "/v1/last/crypto/{from}/{to}"): keys(request("from"), "Timestamp")},
)
FUTURES_TRADES = DatasetSpec(
    name="futures.trades",
    summary="A dataset for the tests.",
    asset_class=AssetClass.FUTURES,
    kind="trades",
    history_path="/futures/v1/trades/{ticker}",
    channels={NotBars.ANY: "T"},
    flat_file_prefixes={NotBars.ANY: "us_futures_{venue}/trades_v1"},
    keys={
        (R, "/futures/v1/trades/{ticker}"): keys(
            request("ticker"), "Timestamp", date="SessionEndDate"
        ),
        # The stream carries no session date, so it declares no date key.
        (W, "futures/T"): keys("Ticker", "Timestamp"),
        (F, "us_futures_{venue}/trades_v1"): keys("Ticker", "Timestamp", date="SessionEndDate"),
    },
    time_axis=("timestamp.gte", "timestamp.lt"),
)
INDEX_VALUES = DatasetSpec(
    name="indices.values",
    summary="A dataset for the tests.",
    asset_class=AssetClass.INDICES,
    kind="values",
    channels={NotBars.ANY: "V"},
    flat_file_prefixes={NotBars.ANY: "us_indices/values_v1"},
    keys={
        (W, "indices/V"): keys("Ticker", "Timestamp"),
        (F, "us_indices/values_v1"): keys("Ticker", "Timestamp"),
    },
)
FOREX_LAST_QUOTE = DatasetSpec(
    name="forex.last_quote",
    summary="A dataset for the tests.",
    asset_class=AssetClass.FOREX,
    kind="quotes",
    fetch_path="/v1/last_quote/currencies/{from}/{to}",
    keys={(R, "/v1/last_quote/currencies/{from}/{to}"): keys(request("from"), "Timestamp")},
)
EXCHANGES = DatasetSpec(
    name="reference.exchanges",
    summary="A dataset for the tests.",
    fetch_path="/v3/reference/exchanges",
    poll_keys=("Id",),
)
CONTRACTS = DatasetSpec(
    name="options.contracts",
    summary="A dataset for the tests.",
    asset_class=AssetClass.OPTIONS,
    fetch_path="/v3/reference/options/contracts",
    extra_paths=("/v3/reference/options/contracts/{options_ticker}",),
    keys={
        (R, "/v3/reference/options/contracts"): keys("Ticker", fanout="UnderlyingTicker"),
        # The per-contract rows carry their ticker, although the request names it too.
        (R, "/v3/reference/options/contracts/{options_ticker}"): keys(
            "Ticker", fanout="UnderlyingTicker"
        ),
    },
    symbol_param="ticker",
)
RELATED = DatasetSpec(
    name="reference.related_companies",
    summary="A dataset for the tests.",
    fetch_path="/v1/related-companies/{ticker}",
    keys={(R, "/v1/related-companies/{ticker}"): keys("Ticker", fanout=request("ticker"))},
    per_symbol=True,
)
DIVIDENDS = DatasetSpec(
    name="stocks.dividends",
    summary="A dataset for the tests.",
    asset_class=AssetClass.STOCKS,
    fetch_path="/stocks/v1/dividends",
    keys={(R, "/stocks/v1/dividends"): keys("Ticker")},
    time_axis=("ex_dividend_date.gte", "ex_dividend_date.lte"),
    symbol_param="ticker.any_of",
)


def without(
    spec: DatasetSpec, source: tuple[Transport, str]
) -> dict[tuple[Transport, str], SourceKeys]:
    return {key: value for key, value in spec.keys.items() if key != source}


def problems(*specs: DatasetSpec) -> tuple[str, ...]:
    return Catalog(specs).problems()


# --- Keys ------------------------------------------------------------------------------------


def test_source_keys_list_their_roles_in_order() -> None:
    declared = keys(
        request("stockTicker"), "WindowStart", fanout="U", date="SessionEndDate", end="E"
    )
    assert declared.declared() == (
        ("symbol", KeySpec("stockTicker", from_request=True)),
        ("time", KeySpec("WindowStart")),
        ("end", KeySpec("E")),
        ("fanout", KeySpec("U")),
        ("date", KeySpec("SessionEndDate")),
    )
    assert SourceKeys().declared() == ()
    assert STOCK_TRADES.keys_for(R, "/nowhere") == SourceKeys()


# --- DatasetSpec -----------------------------------------------------------------------------


def test_a_spec_must_match_a_query_class() -> None:
    stream = DatasetSpec(
        name="test.stream",
        summary="A dataset for the tests.",
        asset_class=AssetClass.STOCKS,
        channels={NotBars.ANY: "T"},
    )
    with pytest.raises(
        ValueError,
        match=r"^test\.stream: a dataset needs channels, a history source, or a fetch path$",
    ):
        dataclasses.replace(stream, channels={})
    own = r"^test\.stream: a fetch path is a dataset of its own, never combined with channels"
    with pytest.raises(ValueError, match=own):
        dataclasses.replace(stream, fetch_path="/v2/last/trade/{stocksTicker}")
    with pytest.raises(ValueError, match=own):
        dataclasses.replace(stream, channels={}, fetch_path="/a", history_path="/b")
    with pytest.raises(ValueError, match=own):  # bars too: the previous bar is a dataset
        dataclasses.replace(
            stream, is_bars=True, channels={}, fetch_path="/prev", history_path="/b"
        )
    with pytest.raises(ValueError, match=r"^test\.stream: a bars dataset needs a history source$"):
        dataclasses.replace(
            stream, is_bars=True, channels={Timespan.MINUTE: "AM"}, fetch_path="/prev"
        )
    # Flat files are a history source too.
    flat = dataclasses.replace(
        stream,
        is_bars=True,
        channels={},
        flat_file_prefixes={Timespan.MINUTE: "us_stocks_sip/minute_aggs_v1"},
        keys={(F, "us_stocks_sip/minute_aggs_v1"): keys("Ticker", "WindowStart")},
    )
    assert (flat.has_history, flat.has_channels, flat.has_fetch) == (True, False, False)


@pytest.mark.parametrize(
    "summary",
    [
        "",
        "No period at the end",
        " Leading whitespace.",
        "Trailing whitespace. ",
        "Two\nlines.",
    ],
)
def test_the_summary_is_one_sentence_ending_in_a_period(summary: str) -> None:
    with pytest.raises(
        ValueError,
        match=r"^reference\.exchanges: the summary must be one sentence ending in a period, got ",
    ):
        dataclasses.replace(EXCHANGES, summary=summary)


def test_a_summary_keeps_its_text() -> None:
    summary = "Trades of US-listed stocks, tick by tick."
    assert dataclasses.replace(EXCHANGES, summary=summary).summary == summary


def test_spec_validation() -> None:
    for name in ("", "trades.", ".trades", "a..b"):  # an empty part anywhere
        with pytest.raises(ValueError, match=r"must be '<namespace>\.<dataset>', or one name"):
            dataclasses.replace(EXCHANGES, name=name)
    # One plain name is a dataset the session serves itself, such as `m.snapshots()`.
    assert dataclasses.replace(EXCHANGES, name="snapshots").name == "snapshots"
    with pytest.raises(ValueError, match=r"test\.x: a WebSocket dataset .* needs a class"):
        DatasetSpec(name="test.x", summary="A dataset for the tests.", channels={NotBars.ANY: "T"})
    with pytest.raises(ValueError, match=r"^test\.x: a flat-file dataset needs a class$"):
        # A file filter looks for the spellings of one class.
        DatasetSpec(
            name="test.x",
            summary="A dataset for the tests.",
            flat_file_prefixes={NotBars.ANY: "us_stocks_sip/trades_v1"},
        )
    with pytest.raises(ValueError, match=r"^stocks\.trades: option_rows is for a dataset of every"):
        dataclasses.replace(STOCK_TRADES, option_rows=True)
    assert dataclasses.replace(EXCHANGES, option_rows=True).option_rows
    as_written = r"^test\.x: symbols kept as written are for REST datasets only$"
    with pytest.raises(ValueError, match=as_written):
        DatasetSpec(
            name="test.x",
            summary="A dataset for the tests.",
            asset_class=AssetClass.STOCKS,
            symbols_as_written=True,
            channels={NotBars.ANY: "T"},
        )
    with pytest.raises(ValueError, match=as_written):
        DatasetSpec(
            name="test.x",
            summary="A dataset for the tests.",
            asset_class=AssetClass.STOCKS,
            symbols_as_written=True,
            flat_file_prefixes={NotBars.ANY: "us/x"},
        )
    with pytest.raises(ValueError, match=r"test\.x: a dataset in a kind needs a class"):
        DatasetSpec(
            name="test.x",
            summary="A dataset for the tests.",
            kind="trades",
            fetch_path="/v3/reference/exchanges",
        )


def test_keys_are_declared_only_for_the_sources_read() -> None:
    with pytest.raises(
        ValueError, match=r"keys are declared for sources it does not read \['WS stocks/Q'\]"
    ):
        dataclasses.replace(
            STOCK_TRADES, keys={**STOCK_TRADES.keys, (W, "stocks/Q"): keys("Ticker")}
        )


ROLE_TIMES = {"end": RowTimes.BAR, "date": RowTimes.TICK}
"""The row times a dataset needs to declare each role at all; the others need none."""


def with_role(role: str, key: KeySpec) -> DatasetSpec:
    # The reference exchanges declaring one key of a role, with the row times the role needs.
    times = ROLE_TIMES.get(role, EXCHANGES.row_times)
    return dataclasses.replace(
        EXCHANGES, row_times=times, keys={(R, "/v3/reference/exchanges"): SourceKeys(**{role: key})}
    )


@pytest.mark.parametrize("role", ["time", "end", "date"])
def test_only_the_symbol_and_fanout_keys_come_from_the_request(role: str) -> None:
    with pytest.raises(ValueError, match=rf"the {role} key cannot come from the request"):
        with_role(role, request("stocksTicker"))


@pytest.mark.parametrize("role", ["symbol", "time", "end", "date"])
def test_only_a_fanout_key_can_be_no_instrument(role: str) -> None:
    with pytest.raises(ValueError, match=r"only a fanout key can be no instrument"):
        with_role(role, KeySpec("Id", instrument=False))


@pytest.mark.parametrize(
    ("role", "spec"),
    [
        ("time", KeySpec("Id", required=False)),
        ("end", KeySpec("Id", required=False)),
        ("date", KeySpec("Id", required=False)),
        ("symbol", KeySpec("stocksTicker", from_request=True, required=False)),
        ("fanout", KeySpec("stocksTicker", from_request=True, required=False)),
    ],
)
def test_only_a_symbol_or_fanout_key_from_rest_rows_can_be_optional(
    role: str, spec: KeySpec
) -> None:
    with pytest.raises(
        ValueError, match=r"only a symbol or fanout key read from a REST path's rows"
    ):
        with_role(role, spec)


def test_a_rest_fanout_key_can_be_optional() -> None:
    # The unified snapshot's underlying: only its options rows carry one.
    spec = with_role("fanout", KeySpec("Id", required=False))
    found = spec.keys_for(R, "/v3/reference/exchanges").fanout
    assert found == KeySpec("Id", required=False)


def test_an_end_key_needs_bars_and_a_date_key_a_dated_dataset() -> None:
    with pytest.raises(ValueError, match=r"only a bars dataset declares an end key"):
        dataclasses.replace(
            STOCK_TRADES, keys={**STOCK_TRADES.keys, (W, "stocks/T"): keys("Ticker", end="E")}
        )
    with pytest.raises(ValueError, match=r"a date key needs a dataset whose rows have a Date"):
        dataclasses.replace(EXCHANGES, keys={(R, "/v3/reference/exchanges"): keys(date="Date")})


def test_a_daily_summary_needs_its_date_key_on_every_source() -> None:
    path = "/v1/open-close/{stocksTicker}/{date}"
    summary = DatasetSpec(
        name="stocks.daily_open_close",
        summary="A dataset for the tests.",
        asset_class=AssetClass.STOCKS,
        fetch_path=path,
        keys={(R, path): keys("Symbol", date="From")},
        per_symbol=True,
        row_times=RowTimes.DAY,
    )
    assert problems(summary) == ()
    with pytest.raises(ValueError, match=r"a daily summary needs a date key on \['REST "):
        dataclasses.replace(summary, keys={(R, path): keys("Symbol")})
    # Its window and descriptions name one calendar zone: futures sessions and data of no
    # class have none.
    for no_zone in (AssetClass.FUTURES, ClassFromPrefix.ANY):
        with pytest.raises(ValueError, match=r"^stocks\.daily_open_close: a daily summary needs"):
            dataclasses.replace(summary, asset_class=no_zone)


def test_every_paged_request_has_a_page_size_and_page_size_is_set_only_where_needed() -> None:
    contracts = "/v3/reference/options/contracts"
    paged = DatasetSpec(
        name="options.contracts",
        summary="A dataset for the tests.",
        asset_class=AssetClass.OPTIONS,
        fetch_path=contracts,
        keys={(R, contracts): keys("Ticker")},
    )
    # The path documents a maximum of 1,000, which the request reads; a dataset may lower it.
    assert problems(paged) == ()
    assert problems(dataclasses.replace(paged, page_size=500)) == ()
    redundant = (
        "page size: options.contracts: page_size 1000 is the documented maximum limit of every "
        "path, so it is read from the specification and must be left unset"
    )
    assert problems(dataclasses.replace(paged, page_size=1000)) == (redundant,)
    too_large = "page size: options.contracts: page_size 1001 exceeds the maximum limit 1000 of "
    too_large += contracts
    assert problems(dataclasses.replace(paged, page_size=1001)) == (too_large,)
    # The aggregates range path states its maximum only in prose, so its dataset sets one.
    bars_path = "/v2/aggs/ticker/{stocksTicker}/range/{multiplier}/{timespan}/{from}/{to}"
    bars = DatasetSpec(
        name="stocks.bars",
        summary="A dataset for the tests.",
        asset_class=AssetClass.STOCKS,
        is_bars=True,
        history_path=bars_path,
        keys={(R, bars_path): keys(request("stocksTicker"), "WindowStart")},
        time_axis=("from", "to"),
    )
    assert problems(bars) == (
        f"page size: stocks.bars: {bars_path} documents no maximum limit, so page_size must be set",
    )
    assert problems(dataclasses.replace(bars, page_size=50_000)) == ()
    open_close = "/v1/open-close/{stocksTicker}/{date}"
    unpaged = DatasetSpec(
        name="stocks.daily_open_close",
        summary="A dataset for the tests.",
        asset_class=AssetClass.STOCKS,
        fetch_path=open_close,
        keys={(R, open_close): keys("Symbol", date="From")},
        per_symbol=True,
        row_times=RowTimes.DAY,
        page_size=10,
    )
    assert problems(unpaged) == (
        "page size: stocks.daily_open_close: page_size is set, but no REST path takes limit",
    )


def test_row_times_follow_the_kind_unless_declared() -> None:
    assert STOCK_TRADES.times is RowTimes.TICK
    assert EXCHANGES.times is RowTimes.NONE
    bars = CATALOG.by_name["stocks.bars"]
    assert bars.row_times is RowTimes.BY_KIND
    assert bars.times is RowTimes.BAR
    assert CATALOG.by_name["indicators.sma"].times is RowTimes.BAR
    # A one-row dataset of the bars kind holds bars without being a bars() dataset; the
    # indicators carry bar times but hold no bars.
    previous = CATALOG.by_name["stocks.previous_day_bar"]
    assert (previous.is_bars, previous.holds_bars, previous.times) == (False, True, RowTimes.BAR)
    assert bars.holds_bars
    assert not CATALOG.by_name["indicators.sma"].holds_bars
    assert not STOCK_TRADES.holds_bars
    assert CATALOG.by_name["stocks.daily_open_close"].times is RowTimes.DAY
    assert dataclasses.replace(EXCHANGES, row_times=RowTimes.TICK).times is RowTimes.TICK
    # No dataset of the catalog resolves to the declaration default.
    assert all(spec.times is not RowTimes.BY_KIND for spec in CATALOG.by_name.values())


def test_a_stream_symbol_cannot_be_optional() -> None:
    optional = keys(KeySpec("Ticker", required=False), "SipTimestamp")
    with pytest.raises(ValueError, match=r"only a symbol or fanout key read from a REST path's"):
        dataclasses.replace(STOCK_TRADES, keys={**STOCK_TRADES.keys, (W, "stocks/T"): optional})


def test_a_rest_symbol_can_be_optional() -> None:
    optional = SourceKeys(symbol=KeySpec("Ticker", required=False))
    spec = dataclasses.replace(EXCHANGES, keys={(R, "/v3/reference/exchanges"): optional})
    assert spec.keys_for(R, "/v3/reference/exchanges").symbol == KeySpec("Ticker", required=False)


@pytest.mark.parametrize("mode", list(PollMode))
def test_no_poll_keys_on_an_optional_symbol(mode: PollMode) -> None:
    # A row without its symbol would have a null poll key, which fails the table in every mode.
    optional = SourceKeys(symbol=KeySpec("Ticker", required=False))
    with pytest.raises(ValueError, match=r"poll_keys cannot hold Symbol when the symbol is"):
        dataclasses.replace(
            EXCHANGES,
            keys={(R, "/v3/reference/exchanges"): optional},
            poll_mode=mode,
            poll_keys=("Symbol", "Id"),
        )
    # Other keys are fine.
    dataclasses.replace(EXCHANGES, keys={(R, "/v3/reference/exchanges"): optional}, poll_mode=mode)


def test_an_append_identity_is_never_empty_and_revisions_never_repeat_poll_keys() -> None:
    with pytest.raises(ValueError, match=r"reference.exchanges: an APPEND dataset needs poll_keys"):
        dataclasses.replace(
            EXCHANGES, poll_keys=(), revision_keys=("Name",), poll_mode=PollMode.APPEND
        )
    with pytest.raises(ValueError, match=r"an explicit APPEND poll\(\) needs a row identity"):
        dataclasses.replace(EXCHANGES, poll_keys=())
    # A one-row dataset keys on nothing, and appends by its revision.
    assert dataclasses.replace(EXCHANGES, poll_keys=(), revision_keys=("Name",)).poll_keys == ()
    with pytest.raises(ValueError, match=r"revision_keys repeat poll_keys \['Id'\]"):
        dataclasses.replace(EXCHANGES, revision_keys=("Name", "Id"))
    assert dataclasses.replace(EXCHANGES, revision_keys=("Name",)).revision_keys == ("Name",)


def test_a_dataset_whose_rows_list_tickers_keys_its_polls_by_symbol() -> None:
    # A row is listed once per named ticker it lists, so Symbol tells its copies apart, and
    # it is null when the call names no ticker.
    match = r"reference.exchanges: a dataset whose rows list their tickers \(Tickers\) needs Symbol"
    with pytest.raises(ValueError, match=match):
        dataclasses.replace(EXCHANGES, ticker_list="Tickers")
    with pytest.raises(ValueError, match=match):
        dataclasses.replace(EXCHANGES, ticker_list="Tickers", poll_keys=("Id", "Symbol"))
    listed = dataclasses.replace(
        EXCHANGES,
        ticker_list="Tickers",
        poll_keys=("Id", "Symbol"),
        nullable_poll_keys={"Symbol": NO_NAMED_TICKER},
    )
    assert listed.ticker_list == "Tickers"


def test_a_fanout_key_that_is_no_instrument_comes_from_the_rows() -> None:
    bad = SourceKeys(fanout=KeySpec("filer_cik", from_request=True, instrument=False))
    with pytest.raises(ValueError, match=r"no instrument keeps a column of the rows"):
        dataclasses.replace(EXCHANGES, keys={(R, "/v3/reference/exchanges"): bad})


def test_flat_files_declare_symbol_and_time_from_their_own_columns() -> None:
    prefix = (F, "us_stocks_sip/trades_v1")
    with pytest.raises(ValueError, match=r"must declare the \['time'\] keys"):
        dataclasses.replace(STOCK_TRADES, keys={**STOCK_TRADES.keys, prefix: keys("Ticker")})
    with pytest.raises(ValueError, match=r"must declare the \['symbol', 'time'\] keys"):
        dataclasses.replace(
            STOCK_TRADES, keys={k: v for k, v in STOCK_TRADES.keys.items() if k != prefix}
        )
    with pytest.raises(ValueError, match=r"must declare the \['symbol'\] keys"):
        dataclasses.replace(
            STOCK_TRADES, keys={**STOCK_TRADES.keys, prefix: keys(request("x"), "SipTimestamp")}
        )


def test_futures_flat_files_declare_their_session() -> None:
    prefix = (F, "us_futures_{venue}/trades_v1")
    with pytest.raises(ValueError, match=r"us_futures_\{venue\}/trades_v1 must declare the \["):
        dataclasses.replace(
            FUTURES_TRADES, keys={**FUTURES_TRADES.keys, prefix: keys("Ticker", "Timestamp")}
        )


def test_bar_channels_and_prefixes_are_keyed_by_timespan() -> None:
    bars = DatasetSpec(
        name="stocks.bars",
        summary="A dataset for the tests.",
        asset_class=AssetClass.STOCKS,
        is_bars=True,
        channels={Timespan.SECOND: "A", Timespan.MINUTE: "AM"},
        history_path="/v2/aggs/ticker/{stocksTicker}/range/{multiplier}/{timespan}/{from}/{to}",
    )
    assert bars.sources[1:] == ((W, "stocks/A"), (W, "stocks/AM"))
    with pytest.raises(ValueError, match="must be keyed by Timespan"):
        dataclasses.replace(bars, channels={NotBars.ANY: "A"})
    with pytest.raises(ValueError, match=r"must be keyed by NotBars\.ANY$"):
        dataclasses.replace(
            EXCHANGES, flat_file_prefixes={Timespan.DAY: "us_stocks_sip/day_aggs_v1"}
        )


def test_a_request_finds_its_source_by_timespan_or_as_not_bars() -> None:
    assert timespan_key(Timespan.MINUTE) is Timespan.MINUTE
    assert timespan_key(None) is NotBars.ANY
    assert STOCK_TRADES.channels[timespan_key(None)] == "T"


def test_sources_are_listed_by_role_then_transport_without_repeats() -> None:
    assert STOCK_TRADES.rest_paths == ("/v3/trades/{stockTicker}",)
    by_class = DatasetSpec(
        name="indicators.sma",
        summary="A dataset for the tests.",
        fetch_path="/v1/indicators/sma/{stockTicker}",
        fetch_paths_by_class={
            AssetClass.CRYPTO: "/v1/indicators/sma/{cryptoTicker}",
            AssetClass.STOCKS: "/v1/indicators/sma/{stockTicker}",
        },
    )
    # The per-class fetch paths follow the fetch path, each once.
    assert by_class.rest_paths == (
        "/v1/indicators/sma/{stockTicker}",
        "/v1/indicators/sma/{cryptoTicker}",
    )
    assert STOCK_TRADES.sources == (
        (R, "/v3/trades/{stockTicker}"),
        (W, "stocks/T"),
        (F, "us_stocks_sip/trades_v1"),
    )
    repeated = dataclasses.replace(EXCHANGES, extra_paths=("/v3/reference/exchanges",))
    assert repeated.sources == ((R, "/v3/reference/exchanges"),)
    assert STOCK_TRADES.group == "stocks"
    assert EXCHANGES.group is None


@pytest.mark.parametrize(
    ("spec", "expected"),
    [
        (STOCK_TRADES, True),  # channels and flat files
        (DIVIDENDS, True),  # a symbol parameter
        (RELATED, True),  # one symbol per request
        (EXCHANGES, False),
        (
            DatasetSpec(
                name="a.b",
                summary="A dataset for the tests.",
                history_path="/v3/trades/{stockTicker}",
            ),
            False,
        ),
        (
            DatasetSpec(
                name="a.b",
                summary="A dataset for the tests.",
                history_path="/v3/trades/{stockTicker}",
                keys={(R, "/v3/trades/{stockTicker}"): keys(request("stockTicker"))},
            ),
            True,  # a symbol filled from the request
        ),
        # A symbol parameter over a string field selects one instrument per row.
        (
            DatasetSpec(
                name="reference.tickers",
                summary="A dataset for the tests.",
                fetch_path="/v3/reference/tickers",
                symbol_param="ticker",
            ),
            True,
        ),
        (
            DatasetSpec(
                name="stocks.snapshots",
                summary="A dataset for the tests.",
                asset_class=AssetClass.STOCKS,
                fetch_path="/v2/snapshot/locale/us/markets/stocks/tickers",
                symbol_param="tickers",
            ),
            True,
        ),
        # One over a list of tickers does not: the rows describe a filing or an article.
        (
            DatasetSpec(
                name="stocks.balance_sheets",
                summary="A dataset for the tests.",
                asset_class=AssetClass.STOCKS,
                fetch_path="/stocks/financials/v1/balance-sheets",
                symbol_param="tickers.any_of",
            ),
            False,
        ),
        (
            DatasetSpec(
                name="reference.news",
                summary="A dataset for the tests.",
                fetch_path="/v2/reference/news",
                symbol_param="ticker",
            ),
            False,
        ),
        # A parameter whose field the rows lack still names the instruments asked for.
        (
            DatasetSpec(
                name="a.b",
                summary="A dataset for the tests.",
                fetch_path="/v3/reference/exchanges",
                symbol_param="mic",
            ),
            True,
        ),
    ],
)
def test_a_dataset_takes_symbols_by_its_transports(spec: DatasetSpec, expected: bool) -> None:
    assert Catalog((spec,)).takes_symbols(spec) is expected


def test_a_filing_keeps_its_tickers_and_has_no_symbol() -> None:
    path = "/stocks/financials/v1/balance-sheets"
    sheets = DatasetSpec(
        name="stocks.balance_sheets",
        summary="A dataset for the tests.",
        asset_class=AssetClass.STOCKS,
        fetch_path=path,
        symbol_param="tickers.any_of",
        poll_keys=("Cik",),
        page_size=100,
    )
    assert problems(sheets) == ()
    columns = Catalog((sheets,)).dataset_schema(sheets).column_names
    assert "Tickers" in columns
    assert "Symbol" not in columns
    # Its list of tickers can never be the symbol.
    assert problems(dataclasses.replace(sheets, keys={(R, path): keys("Tickers")})) == (
        (
            "declared keys: stocks.balance_sheets: REST /stocks/financials/v1/balance-sheets: the "
            "symbol key 'Tickers' is string-array, not a string"
        ),
    )


def test_rest_capabilities() -> None:
    assert not EXCHANGES.has_rest_history
    assert not EXCHANGES.has_grouped_daily
    assert EXCHANGES.has_fetch
    bars = DatasetSpec(
        name="stocks.bars",
        summary="A dataset for the tests.",
        asset_class=AssetClass.STOCKS,
        is_bars=True,
        history_path="/range",
        grouped_daily_path="/grouped",
    )
    assert bars.has_rest_history
    assert bars.has_grouped_daily
    assert not bars.has_fetch


def test_specs_are_identity_hashed() -> None:
    a, b = dataclasses.replace(EXCHANGES), dataclasses.replace(EXCHANGES)
    assert a != b
    assert len({a, b}) == 2


# --- Catalog ---------------------------------------------------------------------------------


def test_the_catalog_holds_the_specs_by_name() -> None:
    other = dataclasses.replace(EXCHANGES, name="reference.other")
    catalog = Catalog((EXCHANGES, other))
    assert list(catalog.by_name) == ["reference.exchanges", "reference.other"]
    assert catalog.by_name["reference.other"] is other
    with pytest.raises(TypeError):
        catalog.by_name["reference.x"] = other  # type: ignore[index]  # the point: read-only


def test_the_catalog_rejects_a_name_declared_twice() -> None:
    with pytest.raises(ValueError, match=r"dataset reference\.exchanges is declared twice"):
        Catalog((EXCHANGES, dataclasses.replace(EXCHANGES)))


def test_the_catalog_reads_the_generated_sources() -> None:
    catalog = Catalog(())
    assert catalog.source(R, "/v3/trades/{stockTicker}") is sources(R)["/v3/trades/{stockTicker}"]
    with pytest.raises(KeyError, match=r"REST /v9/x is not generated: deprecated or removed"):
        catalog.source(R, "/v9/x")


def test_members_follow_the_order_of_their_memberships() -> None:
    # Declared crypto first; the stocks group comes first, by asset class.
    catalog = Catalog((CRYPTO_TRADES, EXCHANGES, STOCK_TRADES))
    assert catalog.members("trades") == (STOCK_TRADES, CRYPTO_TRADES)
    assert catalog.members("quotes") == ()


def test_members_of_one_group_follow_their_positions() -> None:
    # Two datasets of one group: the one whose source comes first in the group comes first.
    last = DatasetSpec(
        name="stocks.last",
        summary="A dataset for the tests.",
        asset_class=AssetClass.STOCKS,
        kind="trades",
        channels={NotBars.ANY: "T"},
        keys={(W, "stocks/T"): keys("Ticker", "SipTimestamp")},
    )
    history = DatasetSpec(
        name="stocks.history",
        summary="A dataset for the tests.",
        asset_class=AssetClass.STOCKS,
        kind="trades",
        history_path="/v3/trades/{stockTicker}",
        keys={(R, "/v3/trades/{stockTicker}"): keys(request("stockTicker"), "SipTimestamp")},
    )
    assert Catalog((last, history)).members("trades") == (history, last)


def test_the_trades_pair_shares_its_columns_in_one_order_with_one_timestamp() -> None:
    catalog = Catalog((STOCK_TRADES, STOCK_LAST_TRADE, CRYPTO_TRADES, CRYPTO_LAST_TRADE))
    stocks, crypto = catalog.dataset_schema(STOCK_TRADES), catalog.dataset_schema(CRYPTO_TRADES)
    # The shared columns have the same names, types, and order; each keeps only its own.
    shared = [(c.name, c.type) for c in stocks.columns if c.name in crypto.column_names]
    assert shared == [(c.name, c.type) for c in crypto.columns if c.name in stocks.column_names]
    assert (stocks.name, crypto.name) == ("stocks.trades", "crypto.trades")
    names = stocks.column_names
    for each in (names, crypto.column_names):
        assert each[:3] == ("Symbol", "Date", "Timestamp")
        assert each[-1] == "TimestampInsert"
        assert each.count("Timestamp") == 1
    # Crypto's participant time is its Timestamp; stocks keeps its own ParticipantTimestamp,
    # which crypto trades do not have. Every source's own symbol key became Symbol.
    assert "ParticipantTimestamp" in names
    assert {"ParticipantTimestamp", "Tape"}.isdisjoint(crypto.column_names)
    assert {"SipTimestamp", "Ticker", "Pair"}.isdisjoint(names)
    types = {c.name: c.type for c in stocks.columns}
    assert types["Timestamp"] is ColumnType.INSTANT
    assert types["Size"] is ColumnType.DOUBLE  # the WebSocket's long joins REST's double
    assert types["Conditions"] is ColumnType.LONG_ARRAY
    assert catalog.problems() == ()


def test_a_projected_member_renames_through_its_projection() -> None:
    schema = Catalog((INDEX_VALUES,)).dataset_schema(INDEX_VALUES)
    assert schema.column_names == ("Symbol", "Date", "Timestamp", "Value", "TimestampInsert")
    assert Catalog((INDEX_VALUES,)).problems() == ()
    # The forex last quote joins the quotes kind with `ask` and `bid` under the kind's names.
    columns = Catalog((FOREX_LAST_QUOTE,)).dataset_schema(FOREX_LAST_QUOTE).column_names
    assert {"AskPrice", "BidPrice"} <= set(columns)
    assert {"Ask", "Bid"}.isdisjoint(columns)


def test_keys_rename_to_symbol_timestamp_and_underlying() -> None:
    catalog = Catalog((CONTRACTS, RELATED, FUTURES_TRADES))
    assert catalog.problems() == ()
    contracts = catalog.dataset_schema(CONTRACTS).column_names
    assert contracts[0] == "Symbol"
    assert "Underlying" in contracts
    assert {"Ticker", "UnderlyingTicker"}.isdisjoint(contracts)
    # The peer is the row's symbol, and the company asked about comes from the request.
    assert catalog.dataset_schema(RELATED).column_names == (
        "Symbol",
        "Underlying",
        "TimestampInsert",
    )
    futures = catalog.dataset_schema(FUTURES_TRADES).column_names
    assert futures[:3] == ("Symbol", "Date", "Timestamp")
    assert "SessionEndDate" not in futures  # the session is the row's Date


def test_a_fanout_key_that_is_no_instrument_keeps_its_column() -> None:
    holdings = DatasetSpec(
        name="stocks.institutional_holdings",
        summary="A dataset for the tests.",
        fetch_path="/stocks/filings/vX/13-F",
        poll_keys=("AccessionNumber",),
        keys={(R, "/stocks/filings/vX/13-F"): keys(fanout=KeySpec("FilerCik", instrument=False))},
    )
    assert problems(holdings) == ()
    columns = Catalog((holdings,)).dataset_schema(holdings).column_names
    # No key is renamed, so none leads: the filer CIK stays where the source puts it.
    assert "FilerCik" in columns
    assert "Underlying" not in columns
    assert columns[0] == "AccessionNumber"
    # The same key as an instrument would be the Underlying.
    as_instrument = dataclasses.replace(
        holdings, keys={(R, "/stocks/filings/vX/13-F"): keys(fanout="FilerCik")}
    )
    renamed = Catalog((as_instrument,)).dataset_schema(as_instrument).column_names
    # The renamed key takes the CIK's place: only Symbol and Timestamp lead.
    assert renamed.index("Underlying") == columns.index("FilerCik")
    assert "FilerCik" not in renamed


def test_a_reference_dataset_declares_no_key() -> None:
    assert problems(EXCHANGES) == ()
    schema = Catalog((EXCHANGES,)).dataset_schema(EXCHANGES)
    assert schema.column_names[0] == "Acronym"  # no key comes first: the source's own order
    assert "Symbol" not in schema.column_names


def test_a_classed_reference_dataset_declares_no_key() -> None:
    # The futures reference datasets keep their class for the session-day rule, but take no
    # symbols, so they are no instrument datasets and need no symbol key.
    exchanges = DatasetSpec(
        name="futures.exchanges",
        summary="A dataset for the tests.",
        asset_class=AssetClass.FUTURES,
        fetch_path="/futures/v1/exchanges",
        poll_keys=("Id",),
    )
    assert not Catalog((exchanges,)).takes_symbols(exchanges)
    assert problems(exchanges) == ()
    # Once it takes symbols, it must say where its rows carry them.
    assert problems(dataclasses.replace(exchanges, symbol_param="mic")) == (
        "declared keys: futures.exchanges: REST /futures/v1/exchanges declares no symbol key",
        (
            "wire parameters: futures.exchanges: symbol parameter 'mic' is not a parameter of "
            "/futures/v1/exchanges"
        ),
    )


def test_dataset_schemas_are_cached_and_the_first_published_wins() -> None:
    # Both threads are inside the merge, past the cache check, before either publishes: the
    # barrier in the first source lookup of each holds them there.
    meet = threading.Barrier(2)
    waited: set[int] = set()
    waited_lock = threading.Lock()

    def tables(transport: Transport) -> Mapping[str, Source]:
        with waited_lock:
            first = threading.get_ident() not in waited
            waited.add(threading.get_ident())
        if first:
            meet.wait(timeout=30)
        return sources(transport)

    catalog = Catalog((STOCK_TRADES, CRYPTO_TRADES), source_tables=tables)
    with ThreadPoolExecutor(2) as pool:
        first, second = pool.map(lambda _: catalog.dataset_schema(STOCK_TRADES), range(2))
    # Each computed its own schema; both return the one that was published first.
    assert first is second
    assert catalog.dataset_schema(STOCK_TRADES) is first


def test_a_kind_dataset_must_be_in_the_catalog() -> None:
    with pytest.raises(LookupError, match=r"stocks\.trades is not in the catalog of its kind"):
        Catalog((CRYPTO_TRADES,)).dataset_schema(STOCK_TRADES)
    # A dataset in no kind is merged alone, in any catalog.
    assert Catalog(()).dataset_schema(EXCHANGES).name == "reference.exchanges"


# --- The catalog gates -----------------------------------------------------------------------


def test_the_process_catalog_passes_every_gate() -> None:
    assert CATALOG.problems() == ()


def test_generated_sources_gate() -> None:
    gone = dataclasses.replace(
        STOCK_TRADES,
        history_path="/v3/trades/{gone}",
        keys=without(STOCK_TRADES, (R, "/v3/trades/{stockTicker}")),
    )
    assert problems(gone) == (
        (
            "generated sources: stocks.trades: REST /v3/trades/{gone} is not generated: "
            "deprecated or removed in the snapshot"
        ),
    )
    # The other gates read the sources, so they wait for this one.
    no_tables = Catalog((EXCHANGES, CRYPTO_TRADES), source_tables=lambda _: {})
    assert len(no_tables.problems()) == 4
    assert all(p.startswith("generated sources: ") for p in no_tables.problems())


def test_declared_keys_gate_needs_the_kind_keys() -> None:
    no_time = {**STOCK_TRADES.keys, (W, "stocks/T"): keys("Ticker")}
    assert problems(dataclasses.replace(STOCK_TRADES, keys=no_time), STOCK_LAST_TRADE) == (
        "declared keys: stocks.trades: WS stocks/T declares no time key",
    )


def test_declared_keys_gate_needs_an_instrument_symbol() -> None:
    assert problems(dataclasses.replace(DIVIDENDS, keys={})) == (
        "declared keys: stocks.dividends: REST /stocks/v1/dividends declares no symbol key",
    )
    # A dataset with no class is an instrument dataset when any source declares a symbol.
    mixed = dataclasses.replace(
        CONTRACTS,
        asset_class=ClassFromPrefix.ANY,
        keys=without(CONTRACTS, (R, "/v3/reference/options/contracts")),
    )
    assert problems(mixed) == (
        (
            "declared keys: options.contracts: REST /v3/reference/options/contracts declares no "
            "symbol key"
        ),
    )


def test_declared_keys_gate_needs_a_column_of_the_source() -> None:
    lacking = {**STOCK_TRADES.keys, (W, "stocks/T"): keys("Ticker", "Timestamp")}
    assert problems(dataclasses.replace(STOCK_TRADES, keys=lacking), STOCK_LAST_TRADE) == (
        (
            "declared keys: stocks.trades: WS stocks/T: the time key 'Timestamp' is not a column "
            "of the source"
        ),
    )
    # A projection decides the names: the last quote's own `ask` is AskPrice in the kind.
    path = "/v1/last_quote/currencies/{from}/{to}"
    unprojected = dataclasses.replace(
        FOREX_LAST_QUOTE, keys={(R, path): keys(request("from"), "Ask")}
    )
    assert problems(unprojected)[0] == (
        f"declared keys: forex.last_quote: REST {path}: the time key 'Ask' is not a column of "
        "the source"
    )


def test_a_per_ticker_path_and_a_futures_channel_pass_the_declared_keys_gate() -> None:
    # The symbol of /v3/trades/{t} comes from the request; the futures stream has no session.
    assert STOCK_TRADES.keys_for(R, "/v3/trades/{stockTicker}").symbol == request("stockTicker")
    assert FUTURES_TRADES.keys_for(W, "futures/T").date is None
    assert problems(STOCK_TRADES, STOCK_LAST_TRADE, FUTURES_TRADES) == ()


def test_unique_columns_gate() -> None:
    # The open-close rows carry a Symbol of their own, so a symbol from the request repeats it.
    open_close = DatasetSpec(
        name="stocks.daily_open_close",
        summary="A dataset for the tests.",
        asset_class=AssetClass.STOCKS,
        fetch_path="/v1/open-close/{stocksTicker}/{date}",
        keys={(R, "/v1/open-close/{stocksTicker}/{date}"): keys(request("stocksTicker"))},
        per_symbol=True,
    )
    assert problems(open_close) == (
        (
            "unique columns: stocks.daily_open_close: REST /v1/open-close/{stocksTicker}/{date}: "
            "columns ['Symbol'] repeat after the rename"
        ),
    )


def test_member_types_gate() -> None:
    # The crypto stream's price as its fan-out joins the stocks stream's text Underlying.
    text = {**STOCK_TRADES.keys, (W, "stocks/T"): keys("Ticker", "SipTimestamp", fanout="Id")}
    priced = {
        **CRYPTO_TRADES.keys,
        (W, "crypto/XT"): keys("Pair", "ParticipantTimestamp", fanout="Price"),
    }
    found = problems(
        dataclasses.replace(STOCK_TRADES, keys=text),
        STOCK_LAST_TRADE,
        dataclasses.replace(CRYPTO_TRADES, keys=priced),
        CRYPTO_LAST_TRADE,
    )
    # Every member of the kind merges the same sources, so each reports the conflict.
    assert found == tuple(
        f"member types: {name}: column 'Underlying' is string elsewhere but double in "
        "crypto.trades WS crypto/XT"
        for name in ("stocks.trades", "stocks.last_trade", "crypto.trades", "crypto.last_trade")
    )


def test_declared_keys_gate_needs_an_instant_as_the_time_key() -> None:
    text_time = {**CRYPTO_TRADES.keys, (W, "crypto/XT"): keys("Pair", "Id")}
    assert problems(dataclasses.replace(CRYPTO_TRADES, keys=text_time), CRYPTO_LAST_TRADE) == (
        (
            "declared keys: crypto.trades: WS crypto/XT: the time key 'Id' is string, not an "
            "instant (epoch or iso-instant)"
        ),
    )
    # A calendar date is no instant either: a date-axis dataset keeps its Date column.
    dated = DatasetSpec(
        name="economy.inflation",
        summary="A dataset for the tests.",
        fetch_path="/fed/v1/inflation",
        keys={(R, "/fed/v1/inflation"): keys(time="Date")},
        poll_keys=("Date",),
    )
    assert problems(dated) == (
        (
            "declared keys: economy.inflation: REST /fed/v1/inflation: the time key 'Date' is "
            "date, not an instant (epoch or iso-instant)"
        ),
    )
    undated = dataclasses.replace(dated, keys={})
    assert problems(undated) == ()
    assert "Date" in Catalog((undated,)).dataset_schema(undated).column_names


def test_fanout_keys_of_datasets_in_no_kind_need_not_agree() -> None:
    # A filer CIK and a related company: two datasets in no kind, each consistent alone.
    holdings = DatasetSpec(
        name="stocks.institutional_holdings",
        summary="A dataset for the tests.",
        fetch_path="/stocks/filings/vX/13-F",
        keys={(R, "/stocks/filings/vX/13-F"): keys(fanout=KeySpec("FilerCik", instrument=False))},
        poll_keys=("AccessionNumber",),
    )
    assert problems(holdings, RELATED) == ()


def test_fanout_keys_agree_on_instrument() -> None:
    contracts = (R, "/v3/reference/options/contracts")
    one = (R, "/v3/reference/options/contracts/{options_ticker}")
    split = dataclasses.replace(
        CONTRACTS,
        keys={
            contracts: keys("Ticker", fanout="UnderlyingTicker"),
            one: keys("Ticker", fanout=KeySpec("UnderlyingTicker", instrument=False)),
        },
    )
    assert problems(split) == (
        "declared keys: options.contracts: its fanout keys disagree on instrument",
    )
    # Across a kind: two members of one kind that disagree.
    as_cik = {
        **STOCK_TRADES.keys,
        (W, "stocks/T"): keys("Ticker", "SipTimestamp", fanout=KeySpec("Id", instrument=False)),
    }
    as_instrument = {
        **CRYPTO_TRADES.keys,
        (W, "crypto/XT"): keys("Pair", "ParticipantTimestamp", fanout="Id"),
    }
    found = problems(
        dataclasses.replace(STOCK_TRADES, keys=as_cik),
        STOCK_LAST_TRADE,
        dataclasses.replace(CRYPTO_TRADES, keys=as_instrument),
        CRYPTO_LAST_TRADE,
    )
    assert found == tuple(
        f"declared keys: {name}: the fanout keys of the trades kind disagree on instrument"
        for name in ("stocks.trades", "stocks.last_trade", "crypto.trades", "crypto.last_trade")
    )


def test_kind_membership_gate() -> None:
    without_files = dataclasses.replace(
        STOCK_TRADES,
        flat_file_prefixes={},
        keys=without(STOCK_TRADES, (F, "us_stocks_sip/trades_v1")),
    )
    assert problems(without_files, STOCK_LAST_TRADE) == (
        (
            "kind membership: stocks.trades: no trades/stocks dataset reads the sources "
            "['FLAT_FILES us_stocks_sip/trades_v1']"
        ),
    )
    # The group's first dataset reports what the group lacks; the one-row dataset alone too.
    assert problems(STOCK_LAST_TRADE) == (
        (
            "kind membership: stocks.last_trade: no trades/stocks dataset reads the sources "
            "['FLAT_FILES us_stocks_sip/trades_v1', 'REST /v3/trades/{stockTicker}', "
            "'WS stocks/T']"
        ),
    )
    outside = dataclasses.replace(
        STOCK_TRADES,
        extra_paths=("/v2/last/nbbo/{stocksTicker}",),
        keys={
            **STOCK_TRADES.keys,
            (R, "/v2/last/nbbo/{stocksTicker}"): keys("Ticker", "SipTimestamp"),
        },
    )
    assert problems(outside, STOCK_LAST_TRADE) == (
        (
            "kind membership: stocks.trades: reads outside it the trades/stocks sources "
            "['REST /v2/last/nbbo/{stocksTicker}']"
        ),
    )
    # Two datasets of one group may not read the same source.
    twice = dataclasses.replace(
        STOCK_LAST_TRADE,
        extra_paths=("/v3/trades/{stockTicker}",),
        keys={
            **STOCK_LAST_TRADE.keys,
            (R, "/v3/trades/{stockTicker}"): keys(request("stockTicker"), "SipTimestamp"),
        },
    )
    assert problems(STOCK_TRADES, twice) == (
        (
            "kind membership: stocks.trades: several trades/stocks datasets read the sources "
            "['REST /v3/trades/{stockTicker}']"
        ),
    )


def test_wire_parameters_gate() -> None:
    bad_axis = dataclasses.replace(STOCK_TRADES, time_axis=("timestamp.gte", "timestamp.before"))
    assert problems(bad_axis, STOCK_LAST_TRADE) == (
        (
            "wire parameters: stocks.trades: time axis 'timestamp.before' is not a parameter of "
            "/v3/trades/{stockTicker}"
        ),
    )
    assert problems(dataclasses.replace(DIVIDENDS, symbol_param="tickers")) == (
        (
            "wire parameters: stocks.dividends: symbol parameter 'tickers' is not a parameter of "
            "/stocks/v1/dividends"
        ),
    )
    elsewhere = dataclasses.replace(DIVIDENDS, symbol_params_elsewhere={"tickers": "use X"})
    assert problems(elsewhere) == (
        (
            "wire parameters: stocks.dividends: symbol parameter 'tickers' is not a parameter of "
            "/stocks/v1/dividends"
        ),
    )
    wrong = {
        **STOCK_TRADES.keys,
        (R, "/v3/trades/{stockTicker}"): keys(request("ticker"), "SipTimestamp"),
    }
    assert problems(dataclasses.replace(STOCK_TRADES, keys=wrong), STOCK_LAST_TRADE) == (
        (
            "wire parameters: stocks.trades: symbol key 'ticker' is not a parameter of "
            "/v3/trades/{stockTicker}"
        ),
    )
    stream = DatasetSpec(
        name="stocks.luld",
        summary="A dataset for the tests.",
        asset_class=AssetClass.STOCKS,
        channels={NotBars.ANY: "LULD"},
        keys={(W, "stocks/LULD"): keys("Ticker")},
        time_axis=("a", "b"),
    )
    assert problems(stream) == ("wire parameters: stocks.luld: a time axis needs a REST path",)
    # The symbol parameter is not needed where the symbol is a path parameter.
    assert problems(CONTRACTS) == ()
    # Every REST path of a dataset takes each parameter the library fixes, as a query parameter.
    assert problems(dataclasses.replace(DIVIDENDS, fixed_params={"frequency": 4})) == ()
    assert problems(dataclasses.replace(DIVIDENDS, fixed_params={"type": "options"})) == (
        (
            "wire parameters: stocks.dividends: fixed parameter 'type' is not a query "
            "parameter of /stocks/v1/dividends"
        ),
    )
    fixed = dataclasses.replace(STOCK_TRADES, fixed_params={"stockTicker": "AAPL"})
    assert problems(fixed, STOCK_LAST_TRADE)[-1] == (
        "wire parameters: stocks.trades: fixed parameter 'stockTicker' is not a query "
        "parameter of /v3/trades/{stockTicker}"
    )


def test_poll_keys_gate() -> None:
    assert problems(dataclasses.replace(EXCHANGES, poll_keys=("Symbol",))) == (
        "poll keys: reference.exchanges: ['Symbol'] are not columns",
    )
    assert problems(dataclasses.replace(EXCHANGES, revision_keys=("Updated", "Name"))) == (
        "poll keys: reference.exchanges: ['Updated'] are not columns",
    )


def test_the_arrival_time_is_never_a_poll_key() -> None:
    with pytest.raises(ValueError, match="poll_keys cannot hold TimestampInsert"):
        dataclasses.replace(EXCHANGES, poll_keys=("Id", "TimestampInsert"))


def test_nullable_poll_keys_are_poll_keys_with_a_reason() -> None:
    with pytest.raises(ValueError, match=r"nullable_poll_keys \['Name'\] are not poll_keys"):
        dataclasses.replace(EXCHANGES, nullable_poll_keys={"Name": "a reason"})
    with pytest.raises(ValueError, match=r"nullable_poll_keys \['Id'\] need a reason"):
        dataclasses.replace(EXCHANGES, poll_keys=("Id", "Name"), nullable_poll_keys={"Id": ""})
    spec = dataclasses.replace(
        EXCHANGES, poll_keys=("Id", "Name"), nullable_poll_keys={"Name": "a reason"}
    )
    assert spec.nullable_poll_keys == {"Name": "a reason"}


def test_nullable_keys_gate() -> None:
    some = dataclasses.replace(
        EXCHANGES, poll_keys=("Id", "Name"), nullable_poll_keys={"Name": "a reason"}
    )
    assert problems(some) == ()
    every = dataclasses.replace(some, nullable_poll_keys={"Id": "a reason", "Name": "a reason"})
    assert problems(every) == (
        "nullable keys: reference.exchanges: every poll key may be null; at least one must not be",
    )
    # A one-row dataset has no poll key to be null.
    assert problems(dataclasses.replace(EXCHANGES, poll_keys=(), revision_keys=("Name",))) == ()


def test_feed_axis_gate() -> None:
    # A feed's poll() needs start, which only a time axis can carry.
    assert problems(dataclasses.replace(EXCHANGES, poll_mode=PollMode.APPEND)) == (
        (
            "feed axis: reference.exchanges: an APPEND dataset declares a time_axis, since its "
            "poll() needs start"
        ),
    )
    assert dataclasses.replace(EXCHANGES, poll_mode=PollMode.APPEND).is_feed
    assert not EXCHANGES.is_feed


def test_append_keys_gate() -> None:
    defaulted = DatasetSpec(
        name="reference.exchanges",
        summary="A dataset for the tests.",
        fetch_path="/v3/reference/exchanges",
        poll_mode=PollMode.APPEND,
    )
    missing = "poll keys: reference.exchanges: ['Symbol'] are not columns"
    assert problems(defaulted) == (
        (
            "append keys: reference.exchanges: an APPEND dataset declares its own poll_keys, "
            "unique per row, not the default ['Symbol']"
        ),
        (
            "feed axis: reference.exchanges: an APPEND dataset declares a time_axis, since its "
            "poll() needs start"
        ),
        missing,
    )
    # A keyed mode may keep the default key.
    assert problems(dataclasses.replace(defaulted, poll_mode=PollMode.UPSERT)) == (missing,)


def test_a_bad_member_is_reported_once() -> None:
    lacking = {**CRYPTO_TRADES.keys, (W, "crypto/XT"): keys("Nope", "ParticipantTimestamp")}
    found = problems(
        STOCK_TRADES,
        STOCK_LAST_TRADE,
        dataclasses.replace(CRYPTO_TRADES, keys=lacking),
        CRYPTO_LAST_TRADE,
    )
    # The stocks merge meets the crypto member's bad key too; the line is listed once.
    assert found == (
        (
            "declared keys: crypto.trades: WS crypto/XT: the symbol key 'Nope' is not a column of "
            "the source"
        ),
    )


# --- Coverage --------------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class Deferral:
    """Why a generated source has no dataset yet, and the GitHub issue that will add one.

    Attributes:
        reason: Why the dataset waits, for the reviewer.
        issue: The number of the open issue that tracks adding the dataset.
    """

    reason: str
    issue: int


DEFERRED: Mapping[tuple[Transport, str], Deferral] = {}
"""Generated sources no dataset reads yet, by `(transport, source key)`.

A source key is the generated key the catalog uses: a REST path template, a WebSocket
`"<cluster>/<channel>"`, or a flat-file prefix. Every entry needs a reason and the number of an
open issue that will add the dataset, for example:

    (Transport.REST, "/v1/example"): Deferral("Pages by cursor only; needs a new verb.", 123),

The coverage test accepts a deferred source, and fails on an entry that names a source the
refresh does not generate or one a dataset already reads, so the list cannot go stale: the pull
request that adds the dataset deletes the entry. Defer only what the library will serve later;
an item it never supports is an `EXCLUSIONS` entry of the refresh instead.
"""


def coverage_problems(
    specs: Iterable[DatasetSpec],
    generated: set[tuple[Transport, str]],
    deferred: Mapping[tuple[Transport, str], Deferral],
) -> list[str]:
    # Every generated source must be read by some dataset, or be deferred with an issue. The
    # refresh generates every item of the vendored snapshot that it neither excludes (with a
    # reason) nor finds deprecated or draft, so together the two checks leave no documented
    # item unaccounted for. The opposite direction, a dataset reading a source nobody
    # generated, is the generated-sources gate.
    used: set[tuple[Transport, str]] = set().union(*(spec.sources for spec in specs))
    problems = [
        f"coverage: {t.value} {key} is generated, but no dataset reads it; add a dataset, an "
        "EXCLUSIONS entry, or a DEFERRED entry"
        for t, key in sorted(generated - used - deferred.keys())
    ]
    for (t, key), deferral in sorted(deferred.items()):
        if (t, key) not in generated:
            problems.append(f"coverage: DEFERRED names {t.value} {key}, which is not generated")
        elif (t, key) in used:
            problems.append(
                f"coverage: DEFERRED names {t.value} {key}, which a dataset reads; remove it"
            )
        if not deferral.reason.strip() or deferral.issue <= 0:
            problems.append(f"coverage: DEFERRED {t.value} {key} needs a reason and an issue")
    return problems


GENERATED = {(t, key) for t in Transport for key in sources(t)}

SourceKey = tuple[Transport, str]
Base = tuple[list[DatasetSpec], dict[SourceKey, Deferral]]


def test_every_generated_source_is_read_by_a_dataset_or_deferred() -> None:
    assert coverage_problems(CATALOG.by_name.values(), GENERATED, DEFERRED) == []


def sole_readers(specs: list[DatasetSpec]) -> list[tuple[DatasetSpec, list[SourceKey]]]:
    # Each dataset that is the only reader of some source, with the sources only it reads.
    readers: dict[SourceKey, int] = {}
    for spec in specs:
        for source in spec.sources:
            readers[source] = readers.get(source, 0) + 1
    found = [(spec, sorted(s for s in spec.sources if readers[s] == 1)) for spec in specs]
    return [(spec, orphaned) for spec, orphaned in found if orphaned]


@pytest.fixture(params=["real", "one-more-deferred"])
def base(request: pytest.FixtureRequest) -> Base:
    # The catalog and the deferrals each negative test starts from: the real ones, and the
    # real ones plus one deferred dataset's sources, so the tests hold whatever DEFERRED holds.
    specs = list(CATALOG.by_name.values())
    if request.param == "real":
        return specs, dict(DEFERRED)
    spec, orphaned = sole_readers(specs)[-1]
    deferred = {**DEFERRED, **dict.fromkeys(orphaned, Deferral("Waits for a new verb.", 1))}
    return [s for s in specs if s is not spec], deferred


def without_a_sole_reader(specs: list[DatasetSpec]) -> tuple[list[DatasetSpec], list[SourceKey]]:
    # The catalog without one dataset that is the only reader of some source, and the sources
    # only it read. Which dataset is chosen does not matter, so a new reader of today's
    # choice moves the test to another dataset instead of breaking it.
    removed, orphaned = sole_readers(specs)[0]
    return [spec for spec in specs if spec is not removed], orphaned


def test_the_bases_pass_the_coverage_test(base: Base) -> None:
    specs, deferred = base
    assert coverage_problems(specs, GENERATED, deferred) == []


def test_coverage_names_a_source_no_dataset_reads(base: Base) -> None:
    specs, orphaned = without_a_sole_reader(base[0])
    assert orphaned
    assert coverage_problems(specs, GENERATED, base[1]) == [
        f"coverage: {t.value} {key} is generated, but no dataset reads it; add a dataset, an "
        "EXCLUSIONS entry, or a DEFERRED entry"
        for t, key in orphaned
    ]
    assert coverage_problems([], set(), {}) == []


def test_coverage_accepts_a_deferred_source(base: Base) -> None:
    specs, orphaned = without_a_sole_reader(base[0])
    deferred = {**base[1], **dict.fromkeys(orphaned, Deferral("Waits for a new verb.", 1))}
    assert coverage_problems(specs, GENERATED, deferred) == []


def test_coverage_refuses_a_deferral_of_a_source_not_generated(base: Base) -> None:
    deferred = {**base[1], (R, "/v1/gone"): Deferral("Waits for a new verb.", 1)}
    assert coverage_problems(base[0], GENERATED, deferred) == [
        "coverage: DEFERRED names REST /v1/gone, which is not generated"
    ]


def test_coverage_refuses_a_deferral_of_a_source_a_dataset_reads(base: Base) -> None:
    _, orphaned = without_a_sole_reader(base[0])
    deferred = {**base[1], orphaned[0]: Deferral("Waits for a new verb.", 1)}
    t, key = orphaned[0]
    assert coverage_problems(base[0], GENERATED, deferred) == [
        f"coverage: DEFERRED names {t.value} {key}, which a dataset reads; remove it"
    ]


@pytest.mark.parametrize(("reason", "issue"), [("", 1), ("  ", 1), ("Waits.", 0)])
def test_coverage_refuses_a_deferral_without_a_reason_or_an_issue(
    base: Base, reason: str, issue: int
) -> None:
    specs, orphaned = without_a_sole_reader(base[0])
    deferred = {**base[1], **dict.fromkeys(orphaned, Deferral(reason, issue))}
    assert coverage_problems(specs, GENERATED, deferred) == [
        f"coverage: DEFERRED {t.value} {key} needs a reason and an issue" for t, key in orphaned
    ]


# --- Filters ---------------------------------------------------------------------------------


INDICATOR = DatasetSpec(
    name="indicators.sma",
    summary="A dataset for the tests.",
    fetch_path="/v1/indicators/sma/{stockTicker}",
)
OPTION_CONTRACTS = dataclasses.replace(CONTRACTS, symbol_param=None)


def test_sources_of_a_catalog_can_be_given() -> None:
    table: dict[str, Source] = {"/v3/reference/exchanges": sources(R)["/v3/reference/exchanges"]}
    given = Catalog((EXCHANGES,), source_tables=lambda t: table if t is R else {})
    assert given.problems() == ()
    assert given.source(R, "/v3/reference/exchanges") is table["/v3/reference/exchanges"]


def test_the_single_class_of_a_dataset_of_every_class_is_a_library_defect() -> None:
    assert STOCK_TRADES.single_class is AssetClass.STOCKS
    with pytest.raises(
        BrokenGuaranteeError,
        match=r"^reference\.exchanges's asset class is ClassFromPrefix, not AssetClass\. ",
    ):
        _ = EXCHANGES.single_class


def test_a_generated_rest_source_that_is_no_rest_path_is_a_library_defect() -> None:
    channel = next(iter(sources(Transport.WS).values()))
    given = Catalog((EXCHANGES,), source_tables=lambda _: {"/v3/reference/exchanges": channel})
    with pytest.raises(
        BrokenGuaranteeError, match=r"^the generated REST source /v3/reference/exch"
    ):
        given.rest_path("/v3/reference/exchanges")


RATINGS = DatasetSpec(
    name="partners.ratings",
    summary="A dataset for the tests.",
    fetch_path="/benzinga/v1/ratings",
)


def test_every_sort_default_massive_documents_passes_the_sort_check() -> None:
    checked = 0
    for spec in CATALOG.by_name.values():
        for path in spec.rest_paths:
            rest_path = sources(R)[path]
            assert isinstance(rest_path, RestPath)
            for param in rest_path.params:
                if param.name == "sort" and isinstance(param.default, str):
                    CATALOG.check_sort(spec, param.default, ordered=False)
                    checked += 1
    assert checked > 40


@pytest.mark.parametrize(
    ("spec", "sort", "ordered", "message"),
    [
        # Massive judges the fields: it answers HTTP 400 to a value an enum sort does not list
        # and to a field its rows lack, and reads a part with a space as the field it names.
        (OPTION_CONTRACTS, "strike_price", True, None),
        (OPTION_CONTRACTS, "strike", False, None),
        (OPTION_CONTRACTS, "strike_price.desc", True, None),
        (RATINGS, "date.desc,ticker", False, None),
        (RATINGS, "date.desc, ticker", False, None),
        (RATINGS, "dat.desc", False, None),
        (RATINGS, "date,,ticker", False, None),
        (RATINGS, "date", True, None),
        (RATINGS, "desc", True, None),
        # A text sort carries its direction as its last part's suffix, so a part's own
        # direction beside `order` cannot be sent.
        (RATINGS, "date.desc", True, r"sort part 'date\.desc' has its own direction"),
        (RATINGS, "ticker,date.asc", True, r"sort part 'date\.asc' has its own direction"),
        # A dataset whose paths take no sort is left to build_query.
        (INDICATOR, "anything", True, None),
    ],
)
def test_only_a_sort_direction_beside_order_is_refused(
    *, spec: DatasetSpec, sort: str, ordered: bool, message: str | None
) -> None:
    catalog = Catalog((spec,))
    if message is None:
        catalog.check_sort(spec, sort, ordered=ordered)
    else:
        with pytest.raises(ValueError, match=rf"^{spec.name}: {message}"):
            catalog.check_sort(spec, sort, ordered=ordered)


def test_no_vendor_field_keeps_a_library_time_column_s_name() -> None:
    # The library owns these names: each such column comes from a declared key (time, end, or
    # date) or is one the library fills, never a vendor field that merely carries the name.
    assert not [p for p in CATALOG.problems() if p.startswith("library names:")]
    for spec in CATALOG.by_name.values():
        names = CATALOG.dataset_schema(spec).column_names
        assert names[-1] == "TimestampInsert"
        timestamps = {n for n in names if re.fullmatch(r"Timestamp(?:[A-Z][A-Za-z0-9]*)?", n)}
        assert timestamps <= LIBRARY_COLUMNS, spec.name


def test_the_naming_gate_catches_a_vendor_timestamp_outside_a_key() -> None:
    # The futures schedules carry a vendor `timestamp`, which the dataset declares as its time
    # key; without the declaration it would be a vendor column named like the library's.
    schedules = CATALOG.by_name["futures.schedules"]
    (path,) = schedules.rest_paths
    assert schedules.keys_for(R, path).time == KeySpec("Timestamp")
    assert (
        f"library names: futures.schedules: REST {path}: the vendor field 'Timestamp' has the "
        "name of a library time column but is no declared key"
    ) in Catalog((dataclasses.replace(schedules, keys={}),)).problems()


@pytest.mark.parametrize(
    ("asset_class", "path", "current", "subscription"),
    [
        (AssetClass.OPTIONS, "/v3/snapshot/options/{u}", True, Subscription.OPTIONS),
        (AssetClass.CRYPTO, "/v1/last/crypto/{from}/{to}", True, Subscription.CURRENCIES),
        (ClassFromPrefix.ANY, "/v3/snapshot", True, None),
        # Past data, which Massive labels DELAYED on a delayed plan though its rows are final.
        (AssetClass.STOCKS, "/v2/aggs/ticker/{stocksTicker}/prev", False, None),
        (AssetClass.STOCKS, None, False, None),
    ],
)
def test_the_current_subscription_is_the_classes_unless_past_or_cross_class(
    *,
    asset_class: AssetClass | ClassFromPrefix,
    path: str | None,
    current: bool,
    subscription: Subscription | None,
) -> None:
    spec = DatasetSpec(
        name="x.y",
        summary="A test dataset.",
        asset_class=asset_class,
        fetch_path=path,
        history_path=None if path else "/v3/trades/{t}",
        serves_current_data=current,
    )
    assert spec.current_subscription is subscription


@pytest.mark.parametrize(
    ("fetch_path", "history_path"),
    [(None, "/v3/trades/{t}"), ("/v3/reference/options/contracts", None)],
    ids=["no fetch path", "reference"],
)
@pytest.mark.parametrize("latest", [False, True], ids=["current", "latest-is-current"])
def test_only_a_market_fetch_path_serves_current_data(
    fetch_path: str | None, history_path: str | None, latest: bool
) -> None:
    with pytest.raises(ValueError, match=r"^x\.y: only a dataset with a fetch path that is no"):
        DatasetSpec(
            name="x.y",
            summary="A test dataset.",
            asset_class=AssetClass.OPTIONS,
            fetch_path=fetch_path,
            history_path=history_path,
            serves_current_data=not latest,
            latest_is_current=latest,
        )


def test_a_dataset_serves_current_data_or_computes_from_it_not_both() -> None:
    with pytest.raises(ValueError, match=r"^x\.y: a dataset serves current data or computes"):
        DatasetSpec(
            name="x.y",
            summary="A test dataset.",
            asset_class=AssetClass.STOCKS,
            fetch_path="/v1/indicators/sma/{stockTicker}",
            serves_current_data=True,
            latest_is_current=True,
        )


def test_the_latest_indicator_values_and_unusual_trades_are_checked_as_current_data() -> None:
    # Each one's latest rows Massive computes from the market's current data.
    latest = {spec.name for spec in CATALOG.by_name.values() if spec.latest_is_current}
    assert latest == {
        "indicators.ema",
        "indicators.macd",
        "indicators.rsi",
        "indicators.sma",
        "options.unusual_trades",
    }


def test_only_datasets_of_current_data_are_checked_for_delayed_labels() -> None:
    # Massive labels past data DELAYED on a plan whose current data is delayed, though its rows
    # are final, so only the market's current state is checked.
    current = {spec.name for spec in CATALOG.by_name.values() if spec.serves_current_data}
    assert current == {
        "crypto.last_trade",
        "crypto.movers",
        "crypto.snapshots",
        "forex.conversion",
        "forex.last_quote",
        "forex.movers",
        "forex.snapshots",
        "futures.snapshots",
        "indices.snapshots",
        "options.chain",
        "options.last_trade",
        "options.snapshots",
        "snapshots",
        "stocks.last_quote",
        "stocks.last_trade",
        "stocks.movers",
        "stocks.snapshots",
    }


def test_each_refused_alternative_is_a_dataset_that_fetches() -> None:
    alternatives = {
        s.name: s.refused_alternative for s in CATALOG.by_name.values() if s.refused_alternative
    }
    assert alternatives == {
        "crypto.last_trade": "crypto.snapshots",
        "forex.last_quote": "forex.snapshots",
        "options.last_trade": "options.snapshots",
        "stocks.last_quote": "stocks.snapshots",
        "stocks.last_trade": "stocks.snapshots",
    }
    assert all(CATALOG.by_name[name].has_fetch for name in alternatives.values())


# --- Streams whose rows name no symbol --------------------------------------------------------

_ADMIN_ARCHIVE = "/options/v1/admin-messages"

ADMIN = DatasetSpec(
    name="options.admin_messages",
    summary="A dataset for the tests.",
    asset_class=AssetClass.OPTIONS,
    kind="admin_messages",
    history_path=_ADMIN_ARCHIVE,
    channels={NotBars.ANY: "AD"},
    keys={
        (R, _ADMIN_ARCHIVE): SourceKeys(time=KeySpec("SipTimestamp")),
        (W, "options/AD"): SourceKeys(time=KeySpec("SipTimestamp")),
    },
    time_axis=("timestamp.gte", "timestamp.lt"),
    names_symbols=False,
)


def test_a_stream_without_symbols_takes_none_and_passes_the_gates() -> None:
    catalog = Catalog((ADMIN,))
    assert catalog.takes_symbols(ADMIN) is False
    assert catalog.problems() == ()  # its default poll key is never read: it has no poll()
    assert "Symbol" not in catalog.dataset_schema(ADMIN).column_names


def test_a_stream_without_symbols_needs_its_time_key() -> None:
    assert problems(dataclasses.replace(ADMIN, keys={})) == (
        (
            "declared keys: options.admin_messages: REST /options/v1/admin-messages declares no "
            "time key"
        ),
        "declared keys: options.admin_messages: WS options/AD declares no time key",
    )


@pytest.mark.parametrize(
    "changed",
    [
        lambda: dataclasses.replace(
            ADMIN, channels={}, fetch_path="/v3/snapshot", kind=None, keys={}
        ),
        lambda: dataclasses.replace(ADMIN, fetch_path="/v3/snapshot"),
        lambda: dataclasses.replace(
            ADMIN, flat_file_prefixes={NotBars.ANY: "us_options_opra/trades_v1"}
        ),
    ],
    ids=["no stream", "fetch", "flat files"],
)
def test_a_dataset_without_symbols_is_a_stream_with_at_most_its_archive(
    changed: Callable[[], DatasetSpec],
) -> None:
    with pytest.raises(ValueError, match="no symbol is a stream, with at most a REST history"):
        changed()


def test_a_stream_without_symbols_needs_no_archive() -> None:
    stream = dataclasses.replace(
        ADMIN,
        history_path=None,
        keys={(W, "options/AD"): SourceKeys(time=KeySpec("SipTimestamp"))},
        time_axis=None,
        kind=None,
    )
    assert problems(stream) == ()


def test_a_dataset_without_symbols_reads_its_archive_by_time_alone() -> None:
    by_ticker = dataclasses.replace(
        ADMIN,
        history_path="/v3/trades/{optionsTicker}",
        keys={(W, "options/AD"): SourceKeys(time=KeySpec("SipTimestamp"))},
        kind=None,
    )
    assert (
        "symbol-free archive: options.admin_messages: its history path takes ['optionsTicker']; "
        "a dataset whose rows name no symbol reads its archive whole"
    ) in problems(by_ticker)


def test_a_stream_without_symbols_declares_no_symbol_key() -> None:
    with pytest.raises(ValueError, match="rows name no symbol has no symbol key"):
        dataclasses.replace(ADMIN, keys={(W, "options/AD"): keys("MessageId", "SipTimestamp")})


def test_the_poll_keys_gate_reads_only_a_dataset_with_poll() -> None:
    # A stream's default `Symbol` poll key is never read; a fetch dataset's must be a column.
    stream = dataclasses.replace(ADMIN, poll_keys=("Nope",))
    assert problems(stream) == ()
    assert problems(dataclasses.replace(EXCHANGES, poll_keys=("Nope",))) == (
        "poll keys: reference.exchanges: ['Nope'] are not columns",
    )


@pytest.mark.parametrize(
    ("key", "role", "transport", "message"),
    [
        (KeySpec("t", from_request=True), "time", Transport.REST, "the time key cannot come"),
        (KeySpec("c", instrument=False), "symbol", Transport.REST, "only a fanout key can be no"),
        (
            KeySpec("c", instrument=False, from_request=True),
            "fanout",
            Transport.REST,
            "a fanout key that is no instrument",
        ),
        (KeySpec("s", required=False), "symbol", Transport.WS, "only a symbol or fanout key"),
        (KeySpec("t", required=False), "time", Transport.REST, "only a symbol or fanout key"),
        (
            KeySpec("s", from_request=True, required=False),
            "symbol",
            Transport.REST,
            "only a symbol or fanout key",
        ),
    ],
    ids=[
        "time-from-request",
        "symbol-no-instrument",
        "no-instrument-from-request",
        "optional-on-ws",
        "optional-time",
        "optional-from-request",
    ],
)
def test_a_key_spec_refuses_a_role_it_cannot_fill(
    *, key: KeySpec, role: str, transport: Transport, message: str
) -> None:
    with pytest.raises(ValueError, match=rf"^d: rest p: {message}"):
        key.check(role, transport, where="d: rest p")


@pytest.mark.parametrize(
    ("key", "role", "transport"),
    [
        (KeySpec("stocksTicker", from_request=True), "symbol", Transport.REST),
        (KeySpec("FilerCik", instrument=False), "fanout", Transport.REST),
        (KeySpec("Ticker", required=False), "symbol", Transport.REST),
        (KeySpec("SipTimestamp"), "time", Transport.WS),
    ],
    ids=["symbol-from-request", "fanout-no-instrument", "optional-rest-symbol", "time"],
)
def test_a_key_spec_accepts_a_role_it_fits(
    *, key: KeySpec, role: str, transport: Transport
) -> None:
    key.check(role, transport, where="d: rest p")


def test_history_columns_refuse_a_transport_history_never_reads() -> None:
    # Only REST and the flat files serve history; any other transport is a defect, never read
    # as the flat files.
    with pytest.raises(BrokenGuaranteeError, match=r"^history\(\) never reads by WS\."):
        CATALOG.history_columns(CATALOG.by_name["stocks.trades"], Transport.WS)


BY_UNDERLYING_PATH = "/options/v1/open-interest/underlying"
BY_UNDERLYING = DatasetSpec(
    name="options.by_underlying",
    summary="A dataset for the tests.",
    asset_class=AssetClass.OPTIONS,
    fetch_path=BY_UNDERLYING_PATH,
    keys={(R, BY_UNDERLYING_PATH): SourceKeys(symbol=KeySpec("UnderlyingTicker"))},
    symbol_param="underlying_ticker.any_of",
    index_prefix=IndexPrefix.BY_UNDERLYING,
)


@pytest.mark.parametrize("prefix", list(IndexPrefix))
def test_each_index_prefix_fits_only_the_datasets_whose_rows_can_tell_an_index(
    prefix: IndexPrefix,
) -> None:
    options_only = "is for an options REST dataset"
    from_rows = "BY_UNDERLYING is for an options REST dataset whose rows carry their symbol"
    stream = dataclasses.replace(
        ADMIN,
        history_path=None,
        keys={(W, "options/AD"): SourceKeys(time=KeySpec("SipTimestamp"))},
        time_axis=None,
    )
    from_request = dataclasses.replace(
        STOCK_TRADES,
        asset_class=ClassFromPrefix.ANY,
        kind=None,
        channels={},
        flat_file_prefixes={},
        keys={k: v for k, v in STOCK_TRADES.keys.items() if k[0] is R},
        time_axis=None,
    )
    match prefix:
        case IndexPrefix.NONE:
            for spec in (CONTRACTS, EXCHANGES, stream, from_request, BY_UNDERLYING):
                assert dataclasses.replace(spec, index_prefix=prefix).index_prefix is prefix
        case IndexPrefix.FROM_CFI | IndexPrefix.BY_ROOT:
            for spec in (CONTRACTS, BY_UNDERLYING):
                assert dataclasses.replace(spec, index_prefix=prefix).index_prefix is prefix
            for spec in (EXCHANGES, stream):
                with pytest.raises(ValueError, match=f"{prefix.value} {options_only}"):
                    dataclasses.replace(spec, index_prefix=prefix)
        case IndexPrefix.BY_UNDERLYING:
            # The data is options data; the symbols are the underlyings, read by prefix.
            assert BY_UNDERLYING.symbols_are_underlyings
            assert BY_UNDERLYING.symbols_class is ClassFromPrefix.ANY
            assert not CONTRACTS.symbols_are_underlyings
            assert CONTRACTS.symbols_class is AssetClass.OPTIONS
            assert dataclasses.replace(CONTRACTS, index_prefix=prefix).index_prefix is prefix
            options_from_request = dataclasses.replace(from_request, asset_class=AssetClass.OPTIONS)
            for spec in (EXCHANGES, stream, from_request, options_from_request):
                with pytest.raises(ValueError, match=from_rows):
                    dataclasses.replace(spec, index_prefix=prefix)
        case _:
            assert_never(prefix)


def test_an_index_prefix_no_case_handles_is_refused() -> None:
    with pytest.raises(AssertionError):
        dataclasses.replace(EXCHANGES, index_prefix="other")  # type: ignore[arg-type]  # the point
