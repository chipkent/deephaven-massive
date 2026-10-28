"""Tests for the dataset catalog: the specs, the merge cache, the catalog gates, and coverage.

The catalog gates run on the process's catalog and, with a negative test each, on synthetic
specs over the real generated sources. The coverage test proves that every generated source
is read by some dataset, or deferred with a reason and an issue in `DEFERRED`.
"""

import dataclasses
import threading
from collections.abc import Iterable, Mapping
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime
from typing import Any, cast

import pytest

from deephaven_massive._datasets import catalog as catalog_module
from deephaven_massive._datasets.catalog import Catalog, DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._datasets.schema import ColumnType
from deephaven_massive._enums import AssetClass, ContractType, PollMode, Timespan
from deephaven_massive._massive import (
    Field,
    ParamKind,
    RestPath,
    Source,
    Transport,
    WireType,
    WireUnit,
    sources,
)
from deephaven_massive._session import CATALOG

R, W, F = Transport.REST, Transport.WS, Transport.FLAT_FILES


def keys(
    symbol: str | KeySpec | None = None,
    time: str | KeySpec | None = None,
    *,
    fanout: str | KeySpec | None = None,
    session: str | KeySpec | None = None,
) -> SourceKeys:
    # A plain string names the source's own column; a KeySpec says more.
    def key(value: str | KeySpec | None) -> KeySpec | None:
        return KeySpec(value) if isinstance(value, str) else value

    return SourceKeys(key(symbol), key(time), key(fanout), key(session))


def request(parameter: str) -> KeySpec:
    return KeySpec(parameter, from_request=True)


STOCK_TRADES = DatasetSpec(
    name="stocks.trades",
    summary="A dataset for the tests.",
    asset_class=AssetClass.STOCKS,
    kind="trades",
    fetch_path="/v2/last/trade/{stocksTicker}",
    history_path="/v3/trades/{stockTicker}",
    channels={None: "T"},
    flat_file_prefixes={None: "us_stocks_sip/trades_v1"},
    keys={
        (R, "/v2/last/trade/{stocksTicker}"): keys("Ticker", "SipTimestamp"),
        (R, "/v3/trades/{stockTicker}"): keys(request("stockTicker"), "SipTimestamp"),
        (W, "stocks/T"): keys("Ticker", "SipTimestamp"),
        (F, "us_stocks_sip/trades_v1"): keys("Ticker", "SipTimestamp"),
    },
    time_axis=("timestamp.gte", "timestamp.lt"),
)
CRYPTO_TRADES = DatasetSpec(
    name="crypto.trades",
    summary="A dataset for the tests.",
    asset_class=AssetClass.CRYPTO,
    kind="trades",
    fetch_path="/v1/last/crypto/{from}/{to}",
    history_path="/v3/trades/{cryptoTicker}",
    channels={None: "XT"},
    flat_file_prefixes={None: "global_crypto/trades_v1"},
    keys={
        (R, "/v1/last/crypto/{from}/{to}"): keys(request("from"), "Timestamp"),
        (R, "/v3/trades/{cryptoTicker}"): keys(request("cryptoTicker"), "ParticipantTimestamp"),
        (W, "crypto/XT"): keys("Pair", "ParticipantTimestamp"),
        (F, "global_crypto/trades_v1"): keys("Ticker", "ParticipantTimestamp"),
    },
    time_axis=("timestamp.gte", "timestamp.lt"),
)
FUTURES_TRADES = DatasetSpec(
    name="futures.trades",
    summary="A dataset for the tests.",
    asset_class=AssetClass.FUTURES,
    kind="trades",
    history_path="/futures/v1/trades/{ticker}",
    channels={None: "T"},
    flat_file_prefixes={None: "us_futures_{venue}/trades_v1"},
    keys={
        (R, "/futures/v1/trades/{ticker}"): keys(
            request("ticker"), "Timestamp", session="SessionEndDate"
        ),
        # The stream carries no session date, so it declares no session key.
        (W, "futures/T"): keys("Ticker", "Timestamp"),
        (F, "us_futures_{venue}/trades_v1"): keys("Ticker", "Timestamp", session="SessionEndDate"),
    },
    time_axis=("timestamp.gte", "timestamp.lt"),
)
INDEX_VALUES = DatasetSpec(
    name="indices.values",
    summary="A dataset for the tests.",
    asset_class=AssetClass.INDICES,
    kind="values",
    fetch_path="/v3/snapshot/indices",
    channels={None: "V"},
    flat_file_prefixes={None: "us_indices/values_v1"},
    keys={
        # `Timestamp` is a column of the snapshot only through its membership of the kind.
        (R, "/v3/snapshot/indices"): keys("Ticker", "Timestamp"),
        (W, "indices/V"): keys("Ticker", "Timestamp"),
        (F, "us_indices/values_v1"): keys("Ticker", "Timestamp"),
    },
    symbol_param="ticker.any_of",
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


def change(spec: DatasetSpec, **changes: object) -> DatasetSpec:
    return dataclasses.replace(spec, **cast("dict[str, Any]", changes))


def without(
    spec: DatasetSpec, source: tuple[Transport, str]
) -> dict[tuple[Transport, str], SourceKeys]:
    return {key: value for key, value in spec.keys.items() if key != source}


def problems(*specs: DatasetSpec) -> tuple[str, ...]:
    return Catalog(specs).problems()


# --- Keys ------------------------------------------------------------------------------------


def test_source_keys_list_their_roles_in_order() -> None:
    declared = keys(request("stockTicker"), "SipTimestamp", session="SessionEndDate")
    assert declared.declared() == (
        ("symbol", KeySpec("stockTicker", from_request=True)),
        ("time", KeySpec("SipTimestamp")),
        ("session", KeySpec("SessionEndDate")),
    )
    assert SourceKeys().declared() == ()
    assert STOCK_TRADES.keys_for(R, "/nowhere") == SourceKeys()


# --- DatasetSpec -----------------------------------------------------------------------------


def test_a_spec_must_match_a_query_class() -> None:
    stream = DatasetSpec(
        name="test.stream",
        summary="A dataset for the tests.",
        asset_class=AssetClass.STOCKS,
        channels={None: "T"},
    )
    with pytest.raises(
        ValueError,
        match=r"^test\.stream: a dataset needs channels, a history source, or a fetch path$",
    ):
        change(stream, channels={})
    with pytest.raises(
        ValueError, match=r"^test\.stream: a dataset with channels and a fetch path needs"
    ):
        change(stream, fetch_path="/v2/last/trade/{stocksTicker}")
    with pytest.raises(ValueError, match=r"^test\.stream: only bars combine a history source"):
        change(stream, channels={}, fetch_path="/a", history_path="/b")
    with pytest.raises(ValueError, match=r"^test\.stream: a bars dataset needs a history source$"):
        change(stream, is_bars=True, channels={Timespan.MINUTE: "AM"}, fetch_path="/prev")
    # Flat files are a history source too.
    flat = change(
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
        change(EXCHANGES, summary=summary)


def test_a_summary_keeps_its_text() -> None:
    summary = "Trades of US-listed stocks, tick by tick."
    assert change(EXCHANGES, summary=summary).summary == summary


def test_spec_validation() -> None:
    for name in ("trades", "trades.", ".trades"):  # no dot, or an empty half on either side
        with pytest.raises(ValueError, match=r"must be '<namespace>\.<dataset>'"):
            change(EXCHANGES, name=name)
    with pytest.raises(ValueError, match=r"test\.x: a WebSocket dataset .* needs a class"):
        DatasetSpec(name="test.x", summary="A dataset for the tests.", channels={None: "T"})
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
        change(STOCK_TRADES, keys={**STOCK_TRADES.keys, (W, "stocks/Q"): keys("Ticker")})


@pytest.mark.parametrize("role", ["time", "session"])
def test_only_the_symbol_and_fanout_keys_come_from_the_request(role: str) -> None:
    bad = SourceKeys(**{role: request("stocksTicker")})
    with pytest.raises(ValueError, match=rf"the {role} key cannot come from the request"):
        change(EXCHANGES, keys={(R, "/v3/reference/exchanges"): bad})


@pytest.mark.parametrize("role", ["symbol", "time", "session"])
def test_only_a_fanout_key_can_be_no_instrument(role: str) -> None:
    bad = SourceKeys(**{role: KeySpec("Id", instrument=False)})
    with pytest.raises(ValueError, match=r"only a fanout key can be no instrument"):
        change(EXCHANGES, keys={(R, "/v3/reference/exchanges"): bad})


@pytest.mark.parametrize(
    ("role", "spec"),
    [
        ("time", KeySpec("Id", required=False)),
        ("fanout", KeySpec("Id", required=False)),
        ("session", KeySpec("Id", required=False)),
        ("symbol", KeySpec("stocksTicker", from_request=True, required=False)),
    ],
)
def test_only_a_symbol_from_rest_rows_can_be_optional(role: str, spec: KeySpec) -> None:
    bad = SourceKeys(**{role: spec})
    with pytest.raises(ValueError, match=r"only a symbol key read from a REST path's rows"):
        change(EXCHANGES, keys={(R, "/v3/reference/exchanges"): bad})


def test_a_stream_symbol_cannot_be_optional() -> None:
    optional = keys(KeySpec("Ticker", required=False), "SipTimestamp")
    with pytest.raises(ValueError, match=r"only a symbol key read from a REST path's rows"):
        change(STOCK_TRADES, keys={**STOCK_TRADES.keys, (W, "stocks/T"): optional})


def test_a_rest_symbol_can_be_optional() -> None:
    optional = SourceKeys(symbol=KeySpec("Ticker", required=False))
    spec = change(EXCHANGES, keys={(R, "/v3/reference/exchanges"): optional})
    assert spec.keys_for(R, "/v3/reference/exchanges").symbol == KeySpec("Ticker", required=False)


@pytest.mark.parametrize("mode", list(PollMode))
def test_no_poll_keys_on_an_optional_symbol(mode: PollMode) -> None:
    # A row without its symbol would have a null poll key, which fails the table in every mode.
    optional = SourceKeys(symbol=KeySpec("Ticker", required=False))
    with pytest.raises(ValueError, match=r"poll_keys cannot hold Symbol when the symbol is"):
        change(
            EXCHANGES,
            keys={(R, "/v3/reference/exchanges"): optional},
            poll_mode=mode,
            poll_keys=("Symbol", "Id"),
        )
    # Other keys are fine.
    change(EXCHANGES, keys={(R, "/v3/reference/exchanges"): optional}, poll_mode=mode)


def test_an_append_identity_is_never_empty_and_revisions_never_repeat_poll_keys() -> None:
    with pytest.raises(ValueError, match=r"reference.exchanges: an APPEND dataset needs poll_keys"):
        change(EXCHANGES, poll_keys=(), revision_keys=("Name",), poll_mode=PollMode.APPEND)
    with pytest.raises(ValueError, match=r"an explicit APPEND poll\(\) needs a row identity"):
        change(EXCHANGES, poll_keys=())
    # A one-row dataset keys on nothing, and appends by its revision.
    assert change(EXCHANGES, poll_keys=(), revision_keys=("Name",)).poll_keys == ()
    with pytest.raises(ValueError, match=r"revision_keys repeat poll_keys \['Id'\]"):
        change(EXCHANGES, revision_keys=("Name", "Id"))
    assert change(EXCHANGES, revision_keys=("Name",)).revision_keys == ("Name",)


def test_a_fanout_key_that_is_no_instrument_comes_from_the_rows() -> None:
    bad = SourceKeys(fanout=KeySpec("filer_cik", from_request=True, instrument=False))
    with pytest.raises(ValueError, match=r"no instrument keeps a column of the rows"):
        change(EXCHANGES, keys={(R, "/v3/reference/exchanges"): bad})


def test_flat_files_declare_symbol_and_time_from_their_own_columns() -> None:
    prefix = (F, "us_stocks_sip/trades_v1")
    with pytest.raises(ValueError, match=r"must declare the \['time'\] keys"):
        change(STOCK_TRADES, keys={**STOCK_TRADES.keys, prefix: keys("Ticker")})
    with pytest.raises(ValueError, match=r"must declare the \['symbol', 'time'\] keys"):
        change(STOCK_TRADES, keys={k: v for k, v in STOCK_TRADES.keys.items() if k != prefix})
    with pytest.raises(ValueError, match=r"must declare the \['symbol'\] keys"):
        change(STOCK_TRADES, keys={**STOCK_TRADES.keys, prefix: keys(request("x"), "SipTimestamp")})


def test_futures_flat_files_declare_their_session() -> None:
    prefix = (F, "us_futures_{venue}/trades_v1")
    with pytest.raises(ValueError, match=r"us_futures_\{venue\}/trades_v1 must declare the \["):
        change(FUTURES_TRADES, keys={**FUTURES_TRADES.keys, prefix: keys("Ticker", "Timestamp")})


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
        change(bars, channels={None: "A"})
    with pytest.raises(ValueError, match="must be keyed by None"):
        change(EXCHANGES, flat_file_prefixes={Timespan.DAY: "us_stocks_sip/day_aggs_v1"})


def test_sources_are_listed_by_role_then_transport_without_repeats() -> None:
    assert STOCK_TRADES.rest_paths == ("/v2/last/trade/{stocksTicker}", "/v3/trades/{stockTicker}")
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
        (R, "/v2/last/trade/{stocksTicker}"),
        (R, "/v3/trades/{stockTicker}"),
        (W, "stocks/T"),
        (F, "us_stocks_sip/trades_v1"),
    )
    repeated = change(EXCHANGES, extra_paths=("/v3/reference/exchanges",))
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
    )
    assert problems(sheets) == ()
    columns = Catalog((sheets,)).dataset_schema(sheets).column_names
    assert "Tickers" in columns
    assert "Symbol" not in columns
    # Its list of tickers can never be the symbol.
    assert problems(change(sheets, keys={(R, path): keys("Tickers")})) == (
        (
            "declared keys: stocks.balance_sheets: REST /stocks/financials/v1/balance-sheets: the "
            "symbol key 'Tickers' is json-text, not a string"
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
    a, b = change(EXCHANGES), change(EXCHANGES)
    assert a != b
    assert len({a, b}) == 2


# --- Catalog ---------------------------------------------------------------------------------


def test_the_catalog_holds_the_specs_by_name() -> None:
    other = change(EXCHANGES, name="reference.other")
    catalog = Catalog((EXCHANGES, other))
    assert list(catalog.by_name) == ["reference.exchanges", "reference.other"]
    assert catalog.by_name["reference.other"] is other
    with pytest.raises(TypeError):
        cast("dict[str, DatasetSpec]", catalog.by_name)["reference.x"] = other


def test_the_catalog_rejects_a_name_declared_twice() -> None:
    with pytest.raises(ValueError, match=r"dataset reference\.exchanges is declared twice"):
        Catalog((EXCHANGES, change(EXCHANGES)))


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
        channels={None: "T"},
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


def test_the_trades_pair_has_identical_columns_with_one_timestamp() -> None:
    catalog = Catalog((STOCK_TRADES, CRYPTO_TRADES))
    stocks, crypto = catalog.dataset_schema(STOCK_TRADES), catalog.dataset_schema(CRYPTO_TRADES)
    # The same names and types; the descriptions are each dataset's own.
    assert [(c.name, c.type) for c in stocks.columns] == [(c.name, c.type) for c in crypto.columns]
    assert (stocks.name, crypto.name) == ("stocks.trades", "crypto.trades")
    names = stocks.column_names
    assert names[:2] == ("Symbol", "Timestamp")
    assert names.count("Timestamp") == 1
    # Crypto's participant time is its Timestamp; stocks keeps its own ParticipantTimestamp,
    # which crypto rows hold null. Every source's own symbol key became Symbol.
    assert "ParticipantTimestamp" in names
    assert {"SipTimestamp", "Ticker", "Pair"}.isdisjoint(names)
    types = {c.name: c.type for c in stocks.columns}
    assert types["Timestamp"] is ColumnType.INSTANT
    assert types["Size"] is ColumnType.DOUBLE  # the WebSocket's long joins REST's double
    assert types["Conditions"] is ColumnType.INT_ARRAY
    assert catalog.problems() == ()


def test_a_projected_member_renames_through_its_projection() -> None:
    schema = Catalog((INDEX_VALUES,)).dataset_schema(INDEX_VALUES)
    # The snapshot joins the values kind with three of its fields; its other fields stay out.
    assert schema.column_names == ("Symbol", "Timestamp", "Value")
    assert Catalog((INDEX_VALUES,)).problems() == ()


def test_keys_rename_to_symbol_timestamp_and_underlying() -> None:
    catalog = Catalog((CONTRACTS, RELATED, FUTURES_TRADES))
    assert catalog.problems() == ()
    contracts = catalog.dataset_schema(CONTRACTS).column_names
    assert contracts[0] == "Symbol"
    assert "Underlying" in contracts
    assert {"Ticker", "UnderlyingTicker"}.isdisjoint(contracts)
    # The peer is the row's symbol, and the company asked about comes from the request.
    assert catalog.dataset_schema(RELATED).column_names == ("Symbol", "Underlying")
    futures = catalog.dataset_schema(FUTURES_TRADES).column_names
    assert futures[:2] == ("Symbol", "Timestamp")
    assert "SessionEndDate" in futures  # the session key keeps its column


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
    as_instrument = change(holdings, keys={(R, "/stocks/filings/vX/13-F"): keys(fanout="FilerCik")})
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
    assert problems(change(exchanges, symbol_param="mic")) == (
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
    gone = change(
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
    assert len(no_tables.problems()) == 5
    assert all(p.startswith("generated sources: ") for p in no_tables.problems())


def test_declared_keys_gate_needs_the_kind_keys() -> None:
    no_time = {**STOCK_TRADES.keys, (W, "stocks/T"): keys("Ticker")}
    assert problems(change(STOCK_TRADES, keys=no_time)) == (
        "declared keys: stocks.trades: WS stocks/T declares no time key",
    )


def test_declared_keys_gate_needs_an_instrument_symbol() -> None:
    assert problems(change(DIVIDENDS, keys={})) == (
        "declared keys: stocks.dividends: REST /stocks/v1/dividends declares no symbol key",
    )
    # A dataset with no class is an instrument dataset when any source declares a symbol.
    mixed = change(
        CONTRACTS,
        asset_class=None,
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
    assert problems(change(STOCK_TRADES, keys=lacking)) == (
        (
            "declared keys: stocks.trades: WS stocks/T: the time key 'Timestamp' is not a column "
            "of the source"
        ),
    )
    # A projection decides the names: the snapshot's own LastUpdated is Timestamp in the kind.
    unprojected = {**INDEX_VALUES.keys, (R, "/v3/snapshot/indices"): keys("Ticker", "LastUpdated")}
    assert problems(change(INDEX_VALUES, keys=unprojected)) == (
        (
            "declared keys: indices.values: REST /v3/snapshot/indices: the time key 'LastUpdated' "
            "is not a column of the source"
        ),
    )


def test_a_per_ticker_path_and_a_futures_channel_pass_the_declared_keys_gate() -> None:
    # The symbol of /v3/trades/{t} comes from the request; the futures stream has no session.
    assert STOCK_TRADES.keys_for(R, "/v3/trades/{stockTicker}").symbol == request("stockTicker")
    assert FUTURES_TRADES.keys_for(W, "futures/T").session is None
    assert problems(STOCK_TRADES, FUTURES_TRADES) == ()


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
    found = problems(change(STOCK_TRADES, keys=text), change(CRYPTO_TRADES, keys=priced))
    assert found == (
        (
            "member types: stocks.trades: column 'Underlying' is string elsewhere but double "
            "in crypto.trades WS crypto/XT"
        ),
        (
            "member types: crypto.trades: column 'Underlying' is string elsewhere but double "
            "in crypto.trades WS crypto/XT"
        ),
    )


def test_declared_keys_gate_needs_an_instant_as_the_time_key() -> None:
    text_time = {**CRYPTO_TRADES.keys, (W, "crypto/XT"): keys("Pair", "Id")}
    assert problems(change(CRYPTO_TRADES, keys=text_time)) == (
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
    undated = change(dated, keys={})
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
    split = change(
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
    found = problems(change(STOCK_TRADES, keys=as_cik), change(CRYPTO_TRADES, keys=as_instrument))
    assert found[:2] == (
        "declared keys: stocks.trades: the fanout keys of the trades kind disagree on instrument",
        "declared keys: crypto.trades: the fanout keys of the trades kind disagree on instrument",
    )


def test_kind_membership_gate() -> None:
    without_files = change(
        STOCK_TRADES,
        flat_file_prefixes={},
        keys=without(STOCK_TRADES, (F, "us_stocks_sip/trades_v1")),
    )
    assert problems(without_files) == (
        (
            "kind membership: stocks.trades: lacks the trades/stocks sources "
            "['FLAT_FILES us_stocks_sip/trades_v1']"
        ),
    )
    outside = change(
        STOCK_TRADES,
        extra_paths=("/v2/last/nbbo/{stocksTicker}",),
        keys={
            **STOCK_TRADES.keys,
            (R, "/v2/last/nbbo/{stocksTicker}"): keys("Ticker", "SipTimestamp"),
        },
    )
    assert problems(outside) == (
        (
            "kind membership: stocks.trades: reads outside it the trades/stocks sources "
            "['REST /v2/last/nbbo/{stocksTicker}']"
        ),
    )


def test_wire_parameters_gate() -> None:
    assert problems(change(STOCK_TRADES, time_axis=("timestamp.gte", "timestamp.before"))) == (
        (
            "wire parameters: stocks.trades: time axis 'timestamp.before' is not a parameter of "
            "/v3/trades/{stockTicker}"
        ),
    )
    assert problems(change(DIVIDENDS, symbol_param="tickers")) == (
        (
            "wire parameters: stocks.dividends: symbol parameter 'tickers' is not a parameter of "
            "/stocks/v1/dividends"
        ),
    )
    wrong = {
        **STOCK_TRADES.keys,
        (R, "/v3/trades/{stockTicker}"): keys(request("ticker"), "SipTimestamp"),
    }
    assert problems(change(STOCK_TRADES, keys=wrong)) == (
        (
            "wire parameters: stocks.trades: symbol key 'ticker' is not a parameter of "
            "/v3/trades/{stockTicker}"
        ),
    )
    stream = DatasetSpec(
        name="stocks.luld",
        summary="A dataset for the tests.",
        asset_class=AssetClass.STOCKS,
        channels={None: "LULD"},
        keys={(W, "stocks/LULD"): keys("Ticker")},
        time_axis=("a", "b"),
    )
    assert problems(stream) == ("wire parameters: stocks.luld: a time axis needs a REST path",)
    # The symbol parameter is not needed where the symbol is a path parameter.
    assert problems(CONTRACTS) == ()
    # Every REST path of a dataset takes each parameter the library fixes, as a query parameter.
    assert problems(change(DIVIDENDS, fixed_params={"frequency": 4})) == ()
    assert problems(change(DIVIDENDS, fixed_params={"type": "options"})) == (
        (
            "wire parameters: stocks.dividends: fixed parameter 'type' is not a query "
            "parameter of /stocks/v1/dividends"
        ),
    )
    assert problems(change(STOCK_TRADES, fixed_params={"stockTicker": "AAPL"}))[-1] == (
        "wire parameters: stocks.trades: fixed parameter 'stockTicker' is not a query "
        "parameter of /v3/trades/{stockTicker}"
    )


def test_poll_keys_gate() -> None:
    assert problems(change(EXCHANGES, poll_keys=("Symbol",))) == (
        "poll keys: reference.exchanges: ['Symbol'] are not columns",
    )
    assert problems(change(EXCHANGES, revision_keys=("Updated", "Name"))) == (
        "poll keys: reference.exchanges: ['Updated'] are not columns",
    )


def test_nullable_poll_keys_are_poll_keys_with_a_reason() -> None:
    with pytest.raises(ValueError, match=r"nullable_poll_keys \['Name'\] are not poll_keys"):
        change(EXCHANGES, nullable_poll_keys={"Name": "a reason"})
    with pytest.raises(ValueError, match=r"nullable_poll_keys \['Id'\] need a reason"):
        change(EXCHANGES, poll_keys=("Id", "Name"), nullable_poll_keys={"Id": ""})
    spec = change(EXCHANGES, poll_keys=("Id", "Name"), nullable_poll_keys={"Name": "a reason"})
    assert spec.nullable_poll_keys == {"Name": "a reason"}


def test_nullable_keys_gate() -> None:
    some = change(EXCHANGES, poll_keys=("Id", "Name"), nullable_poll_keys={"Name": "a reason"})
    assert problems(some) == ()
    every = change(some, nullable_poll_keys={"Id": "a reason", "Name": "a reason"})
    assert problems(every) == (
        "nullable keys: reference.exchanges: every poll key may be null; at least one must not be",
    )
    # A one-row dataset has no poll key to be null.
    assert problems(change(EXCHANGES, poll_keys=(), revision_keys=("Name",))) == ()


def test_append_keys_gate() -> None:
    assert problems(change(EXCHANGES, poll_mode=PollMode.APPEND)) == ()
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
        missing,
    )
    # A keyed mode may keep the default key.
    assert problems(change(defaulted, poll_mode=PollMode.UPSERT)) == (missing,)


def test_a_bad_member_is_reported_once() -> None:
    lacking = {**CRYPTO_TRADES.keys, (W, "crypto/XT"): keys("Nope", "ParticipantTimestamp")}
    found = problems(STOCK_TRADES, change(CRYPTO_TRADES, keys=lacking))
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


DEFERRED: Mapping[tuple[Transport, str], Deferral] = {
    # (Transport.REST, "/v1/example"): Deferral("Pages by cursor only; needs a new verb.", 123),
}
"""Generated sources no dataset reads yet, by `(transport, source key)`.

A source key is the generated key the catalog uses: a REST path template, a WebSocket
`"<cluster>/<channel>"`, or a flat-file prefix. Every entry needs a reason and an issue number.
The coverage test accepts a deferred source, and fails on an entry that names a source the
refresh does not generate or one a dataset already reads, so the list cannot go stale.
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


def test_every_generated_source_is_read_by_a_dataset_or_deferred() -> None:
    assert coverage_problems(CATALOG.by_name.values(), GENERATED, DEFERRED) == []


def sole_reader() -> tuple[list[DatasetSpec], list[tuple[Transport, str]]]:
    # The catalog without one dataset that is the only reader of some source, and the sources
    # only it read. Which dataset is chosen does not matter, so a new reader of today's
    # choice moves the test to another dataset instead of breaking it.
    specs = list(CATALOG.by_name.values())
    readers: dict[tuple[Transport, str], int] = {}
    for spec in specs:
        for source in spec.sources:
            readers[source] = readers.get(source, 0) + 1
    removed = next(spec for spec in specs if any(readers[s] == 1 for s in spec.sources))
    orphaned = sorted(s for s in removed.sources if readers[s] == 1)
    return [spec for spec in specs if spec is not removed], orphaned


def test_coverage_names_a_source_no_dataset_reads() -> None:
    specs, orphaned = sole_reader()
    assert orphaned
    assert coverage_problems(specs, GENERATED, {}) == [
        f"coverage: {t.value} {key} is generated, but no dataset reads it; add a dataset, an "
        "EXCLUSIONS entry, or a DEFERRED entry"
        for t, key in orphaned
    ]
    assert coverage_problems([], set(), {}) == []


def test_coverage_accepts_a_deferred_source() -> None:
    specs, orphaned = sole_reader()
    deferred = dict.fromkeys(orphaned, Deferral("Waits for a new verb.", 1))
    assert coverage_problems(specs, GENERATED, deferred) == []


def test_coverage_refuses_a_deferral_of_a_source_not_generated() -> None:
    deferred = {(R, "/v1/gone"): Deferral("Waits for a new verb.", 1)}
    assert coverage_problems(CATALOG.by_name.values(), GENERATED, deferred) == [
        "coverage: DEFERRED names REST /v1/gone, which is not generated"
    ]


def test_coverage_refuses_a_deferral_of_a_source_a_dataset_reads() -> None:
    _, orphaned = sole_reader()
    deferred = {orphaned[0]: Deferral("Waits for a new verb.", 1)}
    t, key = orphaned[0]
    assert coverage_problems(CATALOG.by_name.values(), GENERATED, deferred) == [
        f"coverage: DEFERRED names {t.value} {key}, which a dataset reads; remove it"
    ]


@pytest.mark.parametrize(("reason", "issue"), [("", 1), ("  ", 1), ("Waits.", 0)])
def test_coverage_refuses_a_deferral_without_a_reason_or_an_issue(reason: str, issue: int) -> None:
    specs, orphaned = sole_reader()
    deferred = dict.fromkeys(orphaned, Deferral(reason, issue))
    assert coverage_problems(specs, GENERATED, deferred) == [
        f"coverage: DEFERRED {t.value} {key} needs a reason and an issue" for t, key in orphaned
    ]


# --- Filters ---------------------------------------------------------------------------------


def check(spec: DatasetSpec, filters: Mapping[str, object]) -> dict[str, object]:
    # The values pass through as given: the check itself must refuse what does not fit.
    return dict(Catalog((spec,)).check_filters(spec, cast("Mapping[str, Any]", filters)))


def test_filters_are_the_query_parameters_of_the_dataset_paths() -> None:
    check(
        DIVIDENDS,
        {
            "frequency_gte": 4,
            "distribution_type": "recurring",
            "distribution_type_any_of": ("recurring", "special"),
        },
    )
    assert check(EXCHANGES, {}) == {}  # nothing to check, so no path is read
    with pytest.raises(ValueError, match=r"has no filter 'pay_date_lte'; valid filters: \["):
        check(DIVIDENDS, {"pay_date_lte": "2025-01-01"})


def test_a_path_parameter_is_no_filter() -> None:
    indicator = DatasetSpec(
        name="indicators.sma",
        summary="A dataset for the tests.",
        fetch_path="/v1/indicators/sma/{stockTicker}",
    )
    check(indicator, {"window": 50})
    with pytest.raises(ValueError, match=r"has no filter 'stockTicker'"):
        check(indicator, {"stockTicker": "AAPL"})


@pytest.mark.parametrize(
    "key",
    [
        "limit",
        "sort",
        # The symbol parameter's family: its base and every suffixed form.
        "ticker",
        "ticker_any_of",
        # The time axis's family, not only the start/end pair.
        "ex_dividend_date",
        "ex_dividend_date_gt",
        "ex_dividend_date_gte",
        "ex_dividend_date_lt",
        "ex_dividend_date_lte",
    ],
)
def test_the_library_sets_some_parameters_itself(key: str) -> None:
    with pytest.raises(ValueError, match=rf"has no filter '{key}'"):
        check(DIVIDENDS, {key: "x"})


def test_the_symbol_parameter_reserves_only_its_equality_forms() -> None:
    # An alphabetical range of tickers is a filter `symbols` cannot express.
    check(DIVIDENDS, {"ticker_gte": "A", "ticker_lt": "B"})
    sheets = DatasetSpec(
        name="stocks.balance_sheets",
        summary="A dataset for the tests.",
        asset_class=AssetClass.STOCKS,
        fetch_path="/stocks/financials/v1/balance-sheets",
        symbol_param="tickers.any_of",
    )
    for key in ("tickers", "tickers_any_of", "tickers_all_of"):
        with pytest.raises(ValueError, match=rf"has no filter '{key}'"):
            check(sheets, {key: "AAPL"})
    assert catalog_module.symbol_family("ticker.any_of") == {
        "ticker",
        "ticker.any_of",
        "ticker.all_of",
    }
    assert "date.all_of" in catalog_module.param_family("date.gte")


def test_filter_values_must_fit_their_kind() -> None:
    with pytest.raises(
        ValueError, match=r"^stocks\.dividends filter 'frequency' takes integer values, not '4'$"
    ):
        check(DIVIDENDS, {"frequency": "4"})
    with pytest.raises(ValueError, match=r"takes one of \['recurring', 'special', .*\], not 'x'"):
        check(DIVIDENDS, {"distribution_type": "x"})
    # Without a time axis, the date parameter is an ordinary filter.
    unaxed = change(DIVIDENDS, time_axis=None)
    check(unaxed, {"ex_dividend_date": date(2025, 1, 2)})
    naive = datetime(2025, 1, 2, 9, 30, tzinfo=UTC).replace(tzinfo=None)  # naive on purpose
    with pytest.raises(ValueError, match=r"^ex_dividend_date=datetime\.datetime\(2025, 1, 2"):
        check(unaxed, {"ex_dividend_date": naive})
    # Text for a date parameter is ISO-8601, and a time in it carries an offset.
    with pytest.raises(ValueError, match=r"^ex_dividend_date='soon' is not an ISO-8601 date"):
        check(unaxed, {"ex_dividend_date": "soon"})
    with pytest.raises(ValueError, match=r"^ex_dividend_date='2025-01-02T09:30' needs a timezone"):
        check(unaxed, {"ex_dividend_date": "2025-01-02T09:30"})
    check(unaxed, {"ex_dividend_date": "2025-01-02T09:30:00Z"})


INDICATOR = DatasetSpec(
    name="indicators.sma",
    summary="A dataset for the tests.",
    fetch_path="/v1/indicators/sma/{stockTicker}",
)
OPTION_CONTRACTS = change(CONTRACTS, symbol_param=None)
NEWS = DatasetSpec(
    name="reference.news",
    summary="A dataset for the tests.",
    fetch_path="/v2/reference/news",
)


@pytest.mark.parametrize(
    ("spec", "key", "given", "kept"),
    [
        (OPTION_CONTRACTS, "underlying_ticker", "NVDA", "NVDA"),
        (DIVIDENDS, "frequency_gte", 4, 4),
        (OPTION_CONTRACTS, "strike_price_gte", 4, 4),
        (OPTION_CONTRACTS, "strike_price_gte", 4.5, 4.5),
        (OPTION_CONTRACTS, "expired", True, True),
        (OPTION_CONTRACTS, "contract_type", "call", "call"),
        (OPTION_CONTRACTS, "contract_type", ContractType.PUT, ContractType.PUT),
        # Text of a date parameter is stored as what it means, never as the text.
        (OPTION_CONTRACTS, "expiration_date_lte", "2026-01-16", date(2026, 1, 16)),
        (OPTION_CONTRACTS, "as_of", date(2025, 9, 18), date(2025, 9, 18)),
        (
            OPTION_CONTRACTS,
            "as_of",
            "2025-09-18T09:30:00-04:00",
            datetime(2025, 9, 18, 13, 30, tzinfo=UTC),
        ),
        # A date on a range filter of an instant parameter stays a date: the REST transport
        # places it in the day zone of each request's symbols. A millisecond timestamp can
        # end a day exactly, so every form takes one.
        (INDICATOR, "timestamp_gte", "2025-01-02", date(2025, 1, 2)),
        (INDICATOR, "timestamp_lte", date(2025, 1, 2), date(2025, 1, 2)),
        (INDICATOR, "timestamp", date(2025, 1, 2), date(2025, 1, 2)),  # no range bound
        (NEWS, "published_utc_gte", "2025-06-02", date(2025, 6, 2)),
        (NEWS, "published_utc_lt", "2025-06-02", date(2025, 6, 2)),
        (NEWS, "published_utc", "2025-06-02", date(2025, 6, 2)),
        # An instant given by the caller is exact already.
        (
            NEWS,
            "published_utc_lte",
            "2025-06-02T12:00:00Z",
            datetime(2025, 6, 2, 12, tzinfo=UTC),
        ),
        # A calendar-date parameter keeps the day.
        (OPTION_CONTRACTS, "expiration_date_gt", "2026-01-16", date(2026, 1, 16)),
        (
            INDICATOR,
            "timestamp_gte",
            "2025-01-02T14:30:00Z",
            datetime(2025, 1, 2, 14, 30, tzinfo=UTC),
        ),
        # A bare text for a list parameter is the list of that one value.
        (DIVIDENDS, "distribution_type_any_of", "special", ("special",)),
        (DIVIDENDS, "distribution_type_any_of", ("a", "b"), ("a", "b")),
        # The wire joins a list with commas, so every spelling of one list is one value.
        (DIVIDENDS, "distribution_type_any_of", "a, b", ("a", "b")),
        (DIVIDENDS, "distribution_type_any_of", ("a,b", "c"), ("a", "b", "c")),
    ],
)
def test_each_parameter_kind_keeps_the_value_it_sends(
    *, spec: DatasetSpec, key: str, given: object, kept: object
) -> None:
    (value,) = check(spec, {key: given}).values()
    assert value == kept
    assert type(value) is type(kept)


@pytest.mark.parametrize(
    ("spec", "key", "value", "message"),
    [
        (OPTION_CONTRACTS, "underlying_ticker", 1, "takes text, not 1"),
        (DIVIDENDS, "frequency_gte", True, "takes integer values, not True"),
        (DIVIDENDS, "frequency_gte", 4.5, "takes integer values, not 4.5"),
        (OPTION_CONTRACTS, "strike_price_gte", False, "takes number values, not False"),
        (OPTION_CONTRACTS, "strike_price_gte", float("nan"), "takes a finite number, not nan"),
        (OPTION_CONTRACTS, "expired", 1, "takes a bool, not 1"),
        (OPTION_CONTRACTS, "contract_type", "up", r"takes one of \['call', 'put'\], not 'up'"),
        (OPTION_CONTRACTS, "as_of", 1, "takes a date or an aware datetime, not 1"),
        (INDICATOR, "timestamp_gte", 1.5, "takes a date or an aware datetime, not 1.5"),
        # Milliseconds cannot carry a microsecond instant; rounding would move the bound.
        (
            INDICATOR,
            "timestamp_gte",
            datetime(2025, 1, 2, 0, 0, 0, 1, tzinfo=UTC),
            "takes whole ms since the epoch",
        ),
        (DIVIDENDS, "distribution_type_any_of", (), "takes at least one value"),
        (DIVIDENDS, "distribution_type_any_of", "a,,b", "has an empty value in"),
        (DIVIDENDS, "distribution_type_any_of", " ", "has an empty value in"),
        # ISO-8601 text of no documented precision cannot end a day exactly.
        (
            NEWS,
            "published_utc_lte",
            date(2025, 6, 2),
            (
                r"published_utc\.lte has no exact unit to end a day with; pass an aware "
                "datetime, or a date to published_utc_gte or published_utc_lt"
            ),
        ),
        (NEWS, "published_utc_gt", "2025-06-02", "no exact unit to end a day with"),
        (DIVIDENDS, "distribution_type_any_of", ("a", 1), "takes a sequence of text"),
        (DIVIDENDS, "distribution_type_any_of", 5, "takes a sequence of text"),
    ],
)
def test_a_value_the_wire_cannot_send_is_refused_at_once(
    *, spec: DatasetSpec, key: str, value: object, message: str
) -> None:
    with pytest.raises(ValueError, match=rf"^{spec.name} filter '{key}'.*{message}"):
        check(spec, {key: value})


def test_sources_of_a_catalog_can_be_given() -> None:
    table: dict[str, Source] = {"/v3/reference/exchanges": sources(R)["/v3/reference/exchanges"]}
    given = Catalog((EXCHANGES,), source_tables=lambda t: table if t is R else {})
    assert given.problems() == ()
    assert given.source(R, "/v3/reference/exchanges") is table["/v3/reference/exchanges"]


RATINGS = DatasetSpec(
    name="partners.ratings",
    summary="A dataset for the tests.",
    fetch_path="/benzinga/v1/ratings",
)


def test_a_list_of_dates_writes_its_date_items_as_days() -> None:
    assert check(RATINGS, {"date_any_of": (date(2025, 6, 2), "2025-06-03,2025-06-04")}) == {
        "date_any_of": ("2025-06-02", "2025-06-03", "2025-06-04")
    }
    with pytest.raises(ValueError, match=r"filter 'ticker_any_of' takes text items, not"):
        check(RATINGS, {"ticker_any_of": (date(2025, 6, 2),)})


def _two_paths(as_text: tuple[str, ...]) -> tuple[Catalog, DatasetSpec]:
    # The dividends path and a copy that takes text for the parameters named `as_text*`.
    real = sources(R)["/stocks/v1/dividends"]
    assert isinstance(real, RestPath)
    params = tuple(
        dataclasses.replace(p, kind=ParamKind.STRING) if p.name.startswith(as_text) else p
        for p in real.params
    )
    copy = dataclasses.replace(real, key="/stocks/v1/dividends-copy", params=params)
    table: dict[str, Source] = {real.key: real, copy.key: copy}
    spec = DatasetSpec(
        name="stocks.two_paths",
        summary="A dataset for the tests.",
        fetch_path=real.key,
        extra_paths=(copy.key,),
    )
    return Catalog((spec,), source_tables=lambda t: table if t is R else {}), spec


def test_every_path_that_declares_a_filter_checks_it() -> None:
    catalog, spec = _two_paths(("frequency", "ex_dividend_date"))
    # The copy takes text where the real path takes an integer: no value suits both.
    with pytest.raises(ValueError, match=r"takes integer values, not '4' \(on /stocks/v1/div"):
        catalog.check_filters(spec, {"frequency": "4"})
    with pytest.raises(ValueError, match=r"takes text, not 4 \(on /stocks/v1/dividends-copy\)"):
        catalog.check_filters(spec, {"frequency": 4})
    # A date parameter on one path and a text one on the other read the text differently.
    with pytest.raises(ValueError, match=r"the dataset's paths read '2025-01-02' as different"):
        catalog.check_filters(spec, {"ex_dividend_date": "2025-01-02"})
    # Paths that agree pass.
    assert catalog.check_filters(spec, {"distribution_type": "special"}) == (
        ("distribution_type", "special"),
    )


def test_an_epoch_field_behind_a_date_parameter_compares_instants() -> None:
    news = sources(R)["/v2/reference/news"]
    assert isinstance(news, RestPath)
    assert catalog_module.takes_instants(news, "published_utc.gte", ParamKind.DATE)
    epoch = Field(
        wire_key="published_utc", column="PublishedUtc", wire_type=WireType.EPOCH, unit=WireUnit.MS
    )
    fields = tuple(epoch if f.wire_key == "published_utc" else f for f in news.fields)
    assert catalog_module.takes_instants(
        dataclasses.replace(news, fields=fields), "published_utc.gte", ParamKind.DATE
    )
    text = Field(wire_key="published_utc", column="PublishedUtc", wire_type=WireType.STRING)
    fields = tuple(text if f.wire_key == "published_utc" else f for f in news.fields)
    assert not catalog_module.takes_instants(
        dataclasses.replace(news, fields=fields), "published_utc.gte", ParamKind.DATE
    )
    assert not catalog_module.takes_instants(news, "ticker", ParamKind.STRING)


def test_the_valid_filters_named_are_only_those_the_caller_can_pass() -> None:
    catalog = Catalog((DIVIDENDS,))
    with pytest.raises(ValueError, match=r"has no filter 'pay_date'") as caught:
        catalog.check_filters(DIVIDENDS, {"pay_date": "2025-01-01"}, typed=("frequency",))
    named = str(caught.value).partition("valid filters: ")[2]
    # The typed parameter's family, the time axis, the symbol's equality forms, and the
    # parameters the library sets are reserved; the symbol's range forms stay filters.
    for reserved in ("'frequency", "'ex_dividend_date", "'ticker'", "'ticker_any_of'", "'limit'"):
        assert reserved not in named
    assert "'ticker_gte'" in named
    assert "'distribution_type'" in named
    with pytest.raises(ValueError, match=r"has no filter 'frequency_gte'"):
        catalog.check_filters(DIVIDENDS, {"frequency_gte": 4}, typed=("frequency",))
