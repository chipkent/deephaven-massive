"""Tests for the `m.forex` namespace: its dataset specs, schemas, factories, and fixtures.

The REST fixtures under `tests/data/namespaces/forex/` are the vendor's documented sample
responses; each is decoded against its generated path to prove it has the generated shape.
"""

import json
from collections.abc import Callable
from datetime import UTC, date, datetime
from typing import cast

import pytest
from deephaven import new_table
from deephaven.column import string_col

import deephaven_massive as dm
from deephaven_massive._datasets.catalog import Catalog, DatasetSpec
from deephaven_massive._datasets.request import Request
from deephaven_massive._datasets.schema import ColumnType
from deephaven_massive._datasets.symbols import SymbolTable
from deephaven_massive._massive import RestPath, Transport, source, to_batch
from deephaven_massive._namespaces import forex
from deephaven_massive._namespaces.base import Namespace, query_class
from deephaven_massive._query import (
    HistoryLatestQuery,
    HistoryQuery,
    Query,
    QueryBase,
    StreamHistoryLatestQuery,
    StreamHistoryQuery,
    StreamQuery,
)
from deephaven_massive._session import CATALOG

R, W, F = Transport.REST, Transport.WS, Transport.FLAT_FILES
DOCS = "https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/forex.md#"

RANGE = "/v2/aggs/ticker/{forexTicker}/range/{multiplier}/{timespan}/{from}/{to}"

# Every source the namespace reads, as the generated files list them.
FOREX_SOURCES = {
    (R, "/v1/conversion/{from}/{to}"),
    (R, "/v1/last_quote/currencies/{from}/{to}"),
    (R, "/v2/aggs/grouped/locale/global/market/fx/{date}"),
    (R, "/v2/aggs/ticker/{forexTicker}/prev"),
    (R, RANGE),
    (R, "/v2/snapshot/locale/global/markets/forex/tickers"),
    (R, "/v2/snapshot/locale/global/markets/forex/{direction}"),
    (R, "/v3/quotes/{fxTicker}"),
    (W, "forex/C"),
    (W, "forex/CA"),
    (W, "forex/CAS"),
    (W, "forex/FMV"),
    (F, "global_forex/day_aggs_v1"),
    (F, "global_forex/minute_aggs_v1"),
    (F, "global_forex/quotes_v1"),
}


def request(query: QueryBase) -> Request:
    return query._request


def test_namespace_is_reached_from_the_session(massive_session: dm.MassiveSession) -> None:
    namespace = massive_session.forex
    assert type(namespace) is forex._Forex
    assert isinstance(namespace, Namespace)


def test_the_namespace_declares_its_six_datasets() -> None:
    names = [spec.name for spec in forex.DATASETS]
    assert names == [
        "forex.quotes",
        "forex.bars",
        "forex.fmv",
        "forex.snapshots",
        "forex.movers",
        "forex.conversion",
    ]
    assert all(CATALOG.by_name[spec.name] is spec for spec in forex.DATASETS)


def test_the_datasets_read_every_forex_source() -> None:
    read = {s for spec in forex.DATASETS for s in spec.sources}
    assert read == FOREX_SOURCES


def test_the_namespace_passes_every_catalog_gate() -> None:
    assert Catalog(forex.DATASETS).problems() == ()
    assert [p for p in CATALOG.problems() if "forex." in p] == []


@pytest.mark.parametrize(
    ("spec", "expected"),
    [
        (forex.QUOTES, StreamHistoryLatestQuery),
        (forex.BARS, HistoryQuery),
        (forex.FMV, StreamQuery),
        (forex.SNAPSHOTS, Query),
        (forex.MOVERS, Query),
        (forex.CONVERSION, Query),
    ],
)
def test_each_dataset_has_its_query_class(spec: DatasetSpec, expected: type) -> None:
    assert query_class(spec) is expected


def _schema(spec: DatasetSpec) -> dict[str, ColumnType]:
    """Return a dataset's columns and types as merged over the forex datasets alone.

    The process catalog also merges the members other namespaces declare into each kind, so
    exact column lists are checked against the namespace's own catalog.
    """
    return {c.name: c.type for c in Catalog(forex.DATASETS).dataset_schema(spec).columns}


def test_quotes_join_the_last_quote_through_its_projection() -> None:
    columns = _schema(forex.QUOTES)
    assert list(columns) == [
        "Symbol",
        "Timestamp",
        "AskExchange",
        "AskPrice",
        "BidExchange",
        "BidPrice",
        "Exchange",
    ]
    assert columns["Timestamp"] is ColumnType.INSTANT
    assert columns["AskPrice"] is ColumnType.DOUBLE
    # The last quote's `ask` and `bid` are the stream's prices, not columns of their own.
    assert {"Ask", "Bid", "Ticker", "ParticipantTimestamp"}.isdisjoint(columns)


def test_bars_rename_the_pair_and_window_start() -> None:
    columns = _schema(forex.BARS)
    assert list(columns)[:2] == ["Symbol", "Timestamp"]
    assert {"Open", "High", "Low", "Close", "Volume", "Vwap", "Transactions"} <= set(columns)
    assert "EndTimestamp" in columns  # the stream's window end
    assert {"Pair", "Ticker", "WindowStart"}.isdisjoint(columns)
    assert columns["Volume"] is ColumnType.DOUBLE  # the stream's long joins REST's double


def test_fmv_holds_the_fair_value_per_pair() -> None:
    assert _schema(forex.FMV) == {
        "Symbol": ColumnType.STRING,
        "Timestamp": ColumnType.INSTANT,
        "Fmv": ColumnType.DOUBLE,
    }


@pytest.mark.parametrize("spec", [forex.SNAPSHOTS, forex.MOVERS])
def test_snapshot_rows_lead_with_their_symbol(spec: DatasetSpec) -> None:
    columns = _schema(spec)
    assert next(iter(columns)) == "Symbol"
    assert "Ticker" not in columns
    assert columns["LastQuoteAskPrice"] is ColumnType.DOUBLE
    assert columns["Updated"] is ColumnType.INSTANT
    assert "Timestamp" not in columns  # a snapshot row keeps its own time columns


def test_conversion_keeps_the_vendor_symbol() -> None:
    columns = _schema(forex.CONVERSION)
    assert next(iter(columns)) == "Symbol"
    assert {"Converted", "InitialAmount", "LastAsk", "LastBid", "LastTimestamp"} <= set(columns)
    assert columns["LastTimestamp"] is ColumnType.INSTANT


# --- factories ------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "symbols", ["EUR/USD", "eur-usd", "EURUSD", "C:EURUSD", ["EUR/USD", "C:EURUSD"]]
)
def test_every_pair_form_becomes_the_canonical_ticker(
    massive_session: dm.MassiveSession, symbols: str | list[str]
) -> None:
    for factory in (massive_session.forex.quotes, massive_session.forex.fmv):
        assert request(factory(symbols)).symbols == ("C:EURUSD",)
    assert request(massive_session.forex.bars(symbols)).symbols == ("C:EURUSD",)
    assert request(massive_session.forex.snapshots(symbols)).symbols == ("C:EURUSD",)


def test_quotes(massive_session: dm.MassiveSession) -> None:
    query = massive_session.forex.quotes(["GBP/USD", "EUR/USD"])
    assert type(query) is StreamHistoryLatestQuery
    assert request(query) == Request(symbols=("C:EURUSD", "C:GBPUSD"))
    assert request(massive_session.forex.quotes(dm.ALL)).symbols is dm.ALL


def test_a_symbol_table_is_a_watchlist(massive_session: dm.MassiveSession) -> None:
    table = new_table([string_col("Symbol", ["EUR/USD"])])
    symbols = request(massive_session.forex.quotes(table)).symbols
    assert isinstance(symbols, SymbolTable)
    assert symbols.table is table


@pytest.mark.parametrize(
    ("case", "expected"),
    [
        ((dm.Timespan.SECOND, 1), StreamHistoryQuery),
        ((dm.Timespan.MINUTE, 1), StreamHistoryQuery),
        ((dm.Timespan.DAY, 1), HistoryLatestQuery),
        ((dm.Timespan.MINUTE, 5), HistoryQuery),
        ((dm.Timespan.HOUR, 1), HistoryQuery),
        ((dm.Timespan.DAY, 2), HistoryQuery),
    ],
)
def test_bars_pick_their_class_from_the_timespan(
    massive_session: dm.MassiveSession, case: tuple[dm.Timespan, int], expected: type
) -> None:
    timespan, multiplier = case
    query = massive_session.forex.bars("EUR/USD", timespan, multiplier=multiplier)
    assert type(query) is expected
    assert request(query) == Request(
        symbols=("C:EURUSD",), timespan=timespan, multiplier=multiplier, adjusted=True
    )


def test_bars_defaults_and_adjusted(massive_session: dm.MassiveSession) -> None:
    query = massive_session.forex.bars(dm.ALL, adjusted=False)
    assert type(query) is StreamHistoryQuery
    assert request(query) == Request(symbols=dm.ALL, timespan=dm.Timespan.MINUTE, adjusted=False)


def test_bars_reject_bad_arguments(massive_session: dm.MassiveSession) -> None:
    bars = massive_session.forex.bars
    with pytest.raises(ValueError, match="SESSION exists only for futures"):
        bars("EUR/USD", dm.Timespan.SESSION)
    with pytest.raises(TypeError, match=r"timespan must be dm\.Timespan"):
        bars("EUR/USD", "minute")  # type: ignore[call-overload]  # a plain string is refused
    with pytest.raises(ValueError, match="multiplier"):
        bars("EUR/USD", dm.Timespan.HOUR, multiplier=0)
    with pytest.raises(TypeError, match="adjusted must be a bool"):
        bars("EUR/USD", adjusted="yes")  # type: ignore[call-overload]  # a str is no bool


def test_fmv(massive_session: dm.MassiveSession) -> None:
    query = massive_session.forex.fmv(dm.ALL)
    assert type(query) is StreamQuery
    assert request(query) == Request(symbols=dm.ALL)


def test_snapshots(massive_session: dm.MassiveSession) -> None:
    query = massive_session.forex.snapshots(dm.ALL)
    assert type(query) is Query
    assert request(query) == Request(symbols=dm.ALL)


def test_movers(massive_session: dm.MassiveSession) -> None:
    query = massive_session.forex.movers(dm.Direction.LOSERS)
    assert type(query) is Query
    assert request(query) == Request(params=(("direction", dm.Direction.LOSERS),))
    with pytest.raises(TypeError, match=r"direction must be dm\.Direction"):
        massive_session.forex.movers("gainers")  # type: ignore[arg-type]  # a str is refused


def test_conversion(massive_session: dm.MassiveSession) -> None:
    query = massive_session.forex.conversion(" aud", "Usd ", amount=100, precision=4)
    assert type(query) is Query
    assert request(query) == Request(
        params=(("amount", 100.0), ("from", "AUD"), ("precision", 4), ("to", "USD"))
    )
    default = request(massive_session.forex.conversion("EUR", "USD"))
    assert dict(default.params) == {"amount": 1.0, "from": "EUR", "precision": 2, "to": "USD"}


@pytest.mark.parametrize(
    ("kwargs", "failure"),
    [
        ({"from_currency": "EURO"}, (ValueError, "from_currency='EURO' is not a three-letter")),
        ({"to_currency": "U$D"}, (ValueError, r"to_currency='U\$D' is not a three-letter")),
        ({"from_currency": 840}, (TypeError, "from_currency must be a str, not int")),
        ({"amount": 0}, (ValueError, "amount must be positive")),
        ({"amount": "1"}, (TypeError, "amount must be a number")),
        ({"precision": 5}, (ValueError, "precision must be 0 to 4, got 5")),
        ({"precision": -1}, (ValueError, "precision must be 0 to 4")),
        ({"precision": 2.0}, (TypeError, "precision must be an int, not float")),
        ({"precision": True}, (TypeError, "precision must be an int, not bool")),
    ],
)
def test_conversion_rejects_bad_arguments(
    massive_session: dm.MassiveSession,
    kwargs: dict[str, object],
    failure: tuple[type[Exception], str],
) -> None:
    error, message = failure
    arguments: dict[str, object] = {"from_currency": "EUR", "to_currency": "USD", **kwargs}
    with pytest.raises(error, match=message):
        massive_session.forex.conversion(**arguments)  # type: ignore[arg-type]  # bad on purpose


@pytest.mark.parametrize(
    ("symbols", "message"),
    [
        ("EUR/US", "cannot read 'EUR/US' as a currency pair"),
        ("EUR/USD/JPY", "cannot read 'EUR/USD/JPY' as a currency pair"),
        ("X:BTCUSD", "carries the prefix of another asset class; forex symbols use the C:"),
        ([], "no symbols"),
    ],
)
def test_invalid_pairs_fail_at_construction(
    massive_session: dm.MassiveSession, symbols: str | list[str], message: str
) -> None:
    forex_ns = massive_session.forex
    factories: list[Callable[[str | list[str]], QueryBase]] = [
        forex_ns.quotes,
        forex_ns.bars,
        forex_ns.fmv,
        forex_ns.snapshots,
    ]
    for factory in factories:
        with pytest.raises(ValueError, match=message):
            factory(symbols)


def test_an_exhausted_generator_has_no_symbols(massive_session: dm.MassiveSession) -> None:
    pairs = (pair for pair in ["EUR/USD"])
    list(pairs)
    with pytest.raises(ValueError, match="no symbols"):
        massive_session.forex.quotes(cast("list[str]", pairs))  # any iterable is read


def test_a_symbol_table_needs_a_symbol_column(massive_session: dm.MassiveSession) -> None:
    table = new_table([string_col("Pair", ["EUR/USD"])])
    with pytest.raises(ValueError, match="must have a Symbol column"):
        massive_session.forex.snapshots(table)


def test_fetch_of_every_pair_names_the_snapshot(massive_session: dm.MassiveSession) -> None:
    with pytest.raises(ValueError, match=r"use m\.forex\.snapshots\(dm\.ALL\)\.fetch\(\)"):
        massive_session.forex.quotes(dm.ALL).fetch()
    with pytest.raises(ValueError, match=r"use m\.forex\.snapshots\(dm\.ALL\)\.fetch\(\)"):
        massive_session.forex.bars(dm.ALL, dm.Timespan.DAY).fetch()


def test_rest_history_of_every_pair_needs_daily_bars(massive_session: dm.MassiveSession) -> None:
    rest = dm.HistorySource.REST
    with pytest.raises(ValueError, match=r"forex\.quotes history over REST needs explicit symbols"):
        massive_session.forex.quotes(dm.ALL).history("2026-10-20", "2026-10-20", source=rest)
    with pytest.raises(ValueError, match=r"forex\.bars history over REST needs explicit symbols"):
        massive_session.forex.bars(dm.ALL, dm.Timespan.HOUR).history(
            date(2026, 10, 20), datetime(2026, 10, 21, tzinfo=UTC), source=rest
        )


def test_every_docstring_starts_with_a_summary_and_links_its_section() -> None:
    methods = {
        "quotes": forex._Forex.quotes,
        "bars": forex._Forex.bars,
        "fmv": forex._Forex.fmv,
        "snapshots": forex._Forex.snapshots,
        "movers": forex._Forex.movers,
        "conversion": forex._Forex.conversion,
    }
    assert {f"forex.{name}" for name in methods} == {spec.name for spec in forex.DATASETS}
    for name, method in methods.items():
        doc = method.__doc__ or ""
        assert f"{DOCS}{name}" in " ".join(doc.split()), name
        assert "columns()" in doc
        assert "limit" in doc
        assert "dm.ALL" in doc


# --- fixtures -------------------------------------------------------------------------------

_ENVELOPE = ("status", "request_id", "next_url", "count")


@pytest.mark.parametrize(
    ("name", "case"),
    [
        ("last_quote.json", (forex.QUOTES, "/v1/last_quote/currencies/{from}/{to}")),
        ("quotes.json", (forex.QUOTES, "/v3/quotes/{fxTicker}")),
        ("prev.json", (forex.BARS, "/v2/aggs/ticker/{forexTicker}/prev")),
        ("range.json", (forex.BARS, RANGE)),
        ("grouped_daily.json", (forex.BARS, "/v2/aggs/grouped/locale/global/market/fx/{date}")),
        ("snapshots.json", (forex.SNAPSHOTS, "/v2/snapshot/locale/global/markets/forex/tickers")),
        ("movers.json", (forex.MOVERS, "/v2/snapshot/locale/global/markets/forex/{direction}")),
        ("conversion.json", (forex.CONVERSION, "/v1/conversion/{from}/{to}")),
    ],
)
def test_each_fixture_has_the_generated_shape(
    load_fixture: Callable[[str, str], bytes], name: str, case: tuple[DatasetSpec, str]
) -> None:
    spec, path = case
    rest_path = source(R, path)
    assert isinstance(rest_path, RestPath)
    body = json.loads(load_fixture("namespaces/forex", name))
    if rest_path.result_key is None:
        rows = [{k: v for k, v in body.items() if k not in _ENVELOPE}]
    else:
        found = body[rest_path.result_key]
        rows = found if isinstance(found, list) else [found]
    declared = spec.keys_for(R, path).declared()
    required = {key.column for _, key in declared if not key.from_request}
    batch, drift = to_batch(rows, rest_path, required=required)
    assert batch.num_rows == len(rows) > 0
    assert (drift.extra, drift.missing) == (frozenset(), frozenset())
