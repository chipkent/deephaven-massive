"""Tests for the dataset catalog: the specs, the merge cache, the catalog gates, and coverage.

The catalog gates run on the process's catalog and, with a negative test each, on synthetic
specs over the real generated sources. The coverage test reads the undeclared-source lists in
`tests/data/datasets/catalog/undeclared/`, one per namespace.
"""

import dataclasses
import threading
from collections.abc import Iterable, Mapping
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, cast

import pytest

from deephaven_massive._datasets import catalog as catalog_module
from deephaven_massive._datasets.catalog import Catalog, DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._datasets.schema import ColumnType, Schema
from deephaven_massive._enums import AssetClass, Order, Timespan
from deephaven_massive._massive import ParamKind, Source, Transport, sources
from deephaven_massive._session import CATALOG, NAMESPACE_MODULES

R, W, F = Transport.REST, Transport.WS, Transport.FLAT_FILES
UNDECLARED = Path(__file__).parents[1] / "data" / "datasets" / "catalog" / "undeclared"


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
    name="reference.exchanges", fetch_path="/v3/reference/exchanges", poll_keys=("Id",)
)
CONTRACTS = DatasetSpec(
    name="options.contracts",
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
    fetch_path="/v1/related-companies/{ticker}",
    keys={(R, "/v1/related-companies/{ticker}"): keys("Ticker", fanout=request("ticker"))},
    per_symbol=True,
)
DIVIDENDS = DatasetSpec(
    name="stocks.dividends",
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
    stream = DatasetSpec(name="test.stream", asset_class=AssetClass.STOCKS, channels={None: "T"})
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


def test_spec_validation() -> None:
    for name in ("trades", "trades.", ".trades"):  # no dot, or an empty half on either side
        with pytest.raises(ValueError, match=r"must be '<namespace>\.<dataset>'"):
            change(EXCHANGES, name=name)
    with pytest.raises(ValueError, match=r"test\.x: a WebSocket dataset .* needs a class"):
        DatasetSpec(name="test.x", channels={None: "T"})
    with pytest.raises(ValueError, match=r"test\.x: a dataset in a kind needs a class"):
        DatasetSpec(name="test.x", kind="trades", fetch_path="/v3/reference/exchanges")


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
    assert STOCK_TRADES.declared_roles == frozenset({"symbol", "time"})
    assert EXCHANGES.declared_roles == frozenset()


def test_rest_capabilities() -> None:
    assert not EXCHANGES.has_rest_history
    assert not EXCHANGES.has_grouped_daily
    assert EXCHANGES.has_fetch
    bars = DatasetSpec(
        name="stocks.bars",
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
        asset_class=AssetClass.STOCKS,
        kind="trades",
        channels={None: "T"},
        keys={(W, "stocks/T"): keys("Ticker", "SipTimestamp")},
    )
    history = DatasetSpec(
        name="stocks.history",
        asset_class=AssetClass.STOCKS,
        kind="trades",
        history_path="/v3/trades/{stockTicker}",
        keys={(R, "/v3/trades/{stockTicker}"): keys(request("stockTicker"), "SipTimestamp")},
    )
    assert Catalog((last, history)).members("trades") == (history, last)


def test_the_trades_pair_has_identical_columns_with_one_timestamp() -> None:
    catalog = Catalog((STOCK_TRADES, CRYPTO_TRADES))
    stocks, crypto = catalog.dataset_schema(STOCK_TRADES), catalog.dataset_schema(CRYPTO_TRADES)
    assert stocks.columns == crypto.columns
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


def test_a_reference_dataset_declares_no_key() -> None:
    assert problems(EXCHANGES) == ()
    schema = Catalog((EXCHANGES,)).dataset_schema(EXCHANGES)
    assert schema.column_names[0] == "Acronym"  # no key comes first: the source's own order
    assert "Symbol" not in schema.column_names


def test_dataset_schemas_are_cached_and_one_winner_is_shared() -> None:
    catalog = Catalog((STOCK_TRADES, CRYPTO_TRADES))
    start = threading.Barrier(16)

    def schema(_: int) -> Schema:
        start.wait()
        return catalog.dataset_schema(STOCK_TRADES)

    with ThreadPoolExecutor(16) as pool:
        results = list(pool.map(schema, range(16)))
    assert all(result is results[0] for result in results)
    assert catalog.dataset_schema(STOCK_TRADES) is results[0]


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
    # The futures REST rows already carry a Timestamp, so another time key repeats it.
    clash = {
        **FUTURES_TRADES.keys,
        (R, "/futures/v1/trades/{ticker}"): keys(request("ticker"), "SequenceNumber"),
    }
    assert problems(change(FUTURES_TRADES, keys=clash)) == (
        (
            "unique columns: futures.trades: REST /futures/v1/trades/{ticker}: columns "
            "['Timestamp'] repeat after the rename"
        ),
    )


def test_member_types_gate() -> None:
    # The crypto stream's text Id as its time joins the kind's epoch Timestamp: a conflict.
    text_time = {**CRYPTO_TRADES.keys, (W, "crypto/XT"): keys("Pair", "Id")}
    crypto = change(CRYPTO_TRADES, keys=text_time)
    found = problems(STOCK_TRADES, crypto)
    assert found == (
        (
            "member types: stocks.trades: column 'Timestamp' is epoch elsewhere but string in "
            "crypto.trades WS crypto/XT"
        ),
        (
            "member types: crypto.trades: column 'Timestamp' is epoch elsewhere but string in "
            "crypto.trades WS crypto/XT"
        ),
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
        asset_class=AssetClass.STOCKS,
        channels={None: "LULD"},
        keys={(W, "stocks/LULD"): keys("Ticker")},
        time_axis=("a", "b"),
    )
    assert problems(stream) == ("wire parameters: stocks.luld: a time axis needs a REST path",)
    # The symbol parameter is not needed where the symbol is a path parameter.
    assert problems(CONTRACTS) == ()


def test_poll_keys_gate() -> None:
    assert problems(change(EXCHANGES, poll_keys=("Symbol",))) == (
        "poll keys: reference.exchanges: ['Symbol'] are not columns",
    )


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


def read_undeclared(directory: Path) -> dict[str, set[tuple[Transport, str]]]:
    listed: dict[str, set[tuple[Transport, str]]] = {}
    for path in sorted(directory.glob("*.txt")):
        lines = [line.strip() for line in path.read_text().splitlines()]
        entries = [line.split(" ", 1) for line in lines if line and not line.startswith("#")]
        listed[path.stem] = {(Transport(t), key) for t, key in entries}
    return listed


def coverage_problems(
    specs: Iterable[DatasetSpec],
    undeclared: Mapping[str, set[tuple[Transport, str]]],
    generated: set[tuple[Transport, str]],
) -> list[str]:
    used: dict[str, set[tuple[Transport, str]]] = {}
    for spec in specs:
        used.setdefault(spec.name.partition(".")[0], set()).update(spec.sources)
    found = []
    every_used = set().union(*used.values()) if used else set()
    every_listed = set().union(*undeclared.values()) if undeclared else set()
    for t, key in sorted(generated - every_used - every_listed):
        found.append(f"coverage: {t.value} {key} is neither used nor listed as undeclared")
    for namespace, listed in sorted(undeclared.items()):
        for t, key in sorted(listed & used.get(namespace, set())):
            found.append(f"coverage: {namespace} already uses {t.value} {key}")
        for t, key in sorted(listed - generated):
            found.append(f"coverage: {namespace} lists {t.value} {key}, which is not generated")
    return found


GENERATED = {(t, key) for t in Transport for key in sources(t)}


def test_every_generated_source_is_used_or_undeclared() -> None:
    assert coverage_problems(CATALOG.by_name.values(), read_undeclared(UNDECLARED), GENERATED) == []


def test_the_undeclared_files_belong_to_namespaces() -> None:
    namespaces = {module.__name__.rpartition(".")[2] for module in NAMESPACE_MODULES}
    assert set(read_undeclared(UNDECLARED)) <= namespaces
    # A source several namespaces will use is listed in each of them.
    listed = read_undeclared(UNDECLARED)
    assert (R, "/v3/snapshot") in listed["options"] & listed["reference"]


def test_coverage_finds_unlisted_used_and_stale_sources() -> None:
    generated = {(R, "/a"), (R, "/b"), (W, "stocks/T")}
    spec = DatasetSpec(name="stocks.x", asset_class=AssetClass.STOCKS, channels={None: "T"})
    undeclared = {"stocks": {(W, "stocks/T"), (R, "/gone")}, "options": {(R, "/a")}}
    assert coverage_problems([spec], undeclared, generated) == [
        "coverage: REST /b is neither used nor listed as undeclared",
        "coverage: stocks already uses WS stocks/T",
        "coverage: stocks lists REST /gone, which is not generated",
    ]
    assert coverage_problems([], {}, set()) == []


# --- Filters ---------------------------------------------------------------------------------


def check(spec: DatasetSpec, filters: Mapping[str, object]) -> None:
    Catalog((spec,)).check_filters(spec, filters)


def test_filters_are_the_query_parameters_of_the_dataset_paths() -> None:
    check(
        DIVIDENDS,
        {
            "frequency_gte": 4,
            "distribution_type": "recurring",
            "distribution_type_any_of": ("recurring", "special"),
            "ticker_gte": "A",
            "ex_dividend_date": date(2025, 1, 2),
        },
    )
    check(EXCHANGES, {})  # nothing to check, so no path is read
    with pytest.raises(ValueError, match=r"has no filter 'pay_date_lte'; valid filters: \["):
        check(DIVIDENDS, {"pay_date_lte": "2025-01-01"})


def test_a_path_parameter_is_no_filter() -> None:
    indicator = DatasetSpec(name="indicators.sma", fetch_path="/v1/indicators/sma/{stockTicker}")
    check(indicator, {"window": 50})
    with pytest.raises(ValueError, match=r"has no filter 'stockTicker'"):
        check(indicator, {"stockTicker": "AAPL"})


@pytest.mark.parametrize(
    "key", ["limit", "sort", "ticker_any_of", "ex_dividend_date_gte", "ex_dividend_date_lte"]
)
def test_the_library_sets_some_parameters_itself(key: str) -> None:
    with pytest.raises(ValueError, match=rf"has no filter '{key}'"):
        check(DIVIDENDS, {key: "x"})


def test_filter_values_must_fit_their_kind() -> None:
    with pytest.raises(ValueError, match=r"filter 'frequency' takes a integer value, not '4'"):
        check(DIVIDENDS, {"frequency": "4"})
    with pytest.raises(ValueError, match=r"takes \['recurring', 'special', .*\], not 'x'"):
        check(DIVIDENDS, {"distribution_type": "x"})
    naive = datetime(2025, 1, 2, 9, 30, tzinfo=UTC).replace(tzinfo=None)  # naive on purpose
    with pytest.raises(ValueError, match=r"^ex_dividend_date=datetime\.datetime\(2025, 1, 2"):
        check(DIVIDENDS, {"ex_dividend_date": naive})
    # Text for a date parameter is ISO-8601, and a time in it carries an offset.
    with pytest.raises(ValueError, match=r"^ex_dividend_date='soon' is not an ISO-8601 date"):
        check(DIVIDENDS, {"ex_dividend_date": "soon"})
    with pytest.raises(ValueError, match=r"^ex_dividend_date='2025-01-02T09:30' needs a timezone"):
        check(DIVIDENDS, {"ex_dividend_date": "2025-01-02T09:30"})
    check(DIVIDENDS, {"ex_dividend_date": "2025-01-02T09:30:00Z"})


@pytest.mark.parametrize(
    ("kind", "good", "bad"),
    [
        (ParamKind.STRING, ["x"], [1, ("x",)]),
        (ParamKind.INTEGER, [4], [True, 4.5, "4"]),
        (ParamKind.NUMBER, [4, 4.5], [False, "4"]),
        (ParamKind.BOOLEAN, [True], [1, "true"]),
        (ParamKind.ENUM, ["asc", Order.DESC], ["up", 1]),
        (ParamKind.DATE, [date(2025, 1, 2), datetime(2025, 1, 2, tzinfo=UTC), "2025"], [1]),
        (ParamKind.TIMESTAMP, [date(2025, 1, 2), "2025-01-02"], [1.5]),
        (ParamKind.LIST, ["a", ("a", "b")], [1, ["a"]]),
    ],
)
def test_each_parameter_kind_takes_its_values(
    kind: ParamKind, good: list[object], bad: list[object]
) -> None:
    for value in good:
        assert catalog_module._fits(kind, ("asc", "desc"), value), value
    for value in bad:
        assert not catalog_module._fits(kind, ("asc", "desc"), value), value


def test_sources_of_a_catalog_can_be_given() -> None:
    table: dict[str, Source] = {"/v3/reference/exchanges": sources(R)["/v3/reference/exchanges"]}
    given = Catalog((EXCHANGES,), source_tables=lambda t: table if t is R else {})
    assert given.problems() == ()
    assert given.source(R, "/v3/reference/exchanges") is table["/v3/reference/exchanges"]
