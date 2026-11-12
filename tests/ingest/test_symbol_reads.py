"""Tests for reads by symbol: each symbol's rows or its own failure, through the transports.

The REST reads run against the fake REST server; the flat-file read is replaced by a scripted
table or failure, since only the split of its rows and failures by symbol is this module's.
"""

from __future__ import annotations

import dataclasses
import logging
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING, Any, cast

import pyarrow as pa
import pytest
from deephaven import arrow as dh_arrow
from deephaven import new_table
from deephaven.column import string_col

import deephaven_massive as dm
from deephaven_massive import _session
from deephaven_massive._datasets import history_source
from deephaven_massive._datasets.catalog import Catalog, DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._datasets.history_source import history_window
from deephaven_massive._datasets.request import Request, ValueTable
from deephaven_massive._enums import AssetClass
from deephaven_massive._errors import (
    LIBRARY_BUG,
    CutShortError,
    EntitlementError,
    ErrorSource,
    MassiveError,
    RowDataError,
    UnlistedPairError,
)
from deephaven_massive._ingest import rate_limits
from deephaven_massive._ingest.symbol_reads import (
    UnsupportedParameterError,
    failure_kind,
    split_by_symbol,
    symbol_outcomes,
)
from deephaven_massive._massive import Transport
from deephaven_massive._tables.columns import dataset_table
from deephaven_massive._tables.execution_context import entered
from deephaven_massive._tables.transports import FailureKind, SymbolFailure, SymbolRows
from deephaven_massive.namespaces import _futures as futures
from tests._fakes.rest import FakeRestServer, Reply, envelope

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping

    from deephaven.table import Table

    from deephaven_massive._datasets.history_source import HistoryWindow
    from deephaven_massive._tables.transports import SymbolOutcome

LOGGER = "deephaven_massive._ingest.symbol_reads"
REST_LOGGER = "deephaven_massive._ingest.rest"
R = Transport.REST
NS = 10**9

DIVIDENDS_PATH = "/stocks/v1/dividends"
DIVIDENDS = DatasetSpec(
    summary="A dataset for the tests.",
    name="stocks.dividends",
    asset_class=AssetClass.STOCKS,
    fetch_path=DIVIDENDS_PATH,
    symbol_param="ticker.any_of",
    keys={(R, DIVIDENDS_PATH): SourceKeys(symbol=KeySpec("Ticker"))},
    symbols_per_request=2,
)
BENZINGA_NEWS_PATH = "/benzinga/v2/news"
# A dataset whose rows list their tickers: the library fills Symbol from them.
LISTED = DatasetSpec(
    summary="A dataset for the tests.",
    name="partners.benzinga.news",
    fetch_path=BENZINGA_NEWS_PATH,
    symbol_param="tickers.any_of",
    ticker_list="Tickers",
    poll_keys=("BenzingaId", "Symbol"),
    nullable_poll_keys={"Symbol": "no ticker named"},
)
CRYPTO_LAST_PATH = "/v1/last/crypto/{from}/{to}"
CRYPTO_LAST = DatasetSpec(
    summary="A dataset for the tests.",
    name="crypto.last",
    asset_class=AssetClass.CRYPTO,
    fetch_path=CRYPTO_LAST_PATH,
    serves_current_data=True,
    keys={(R, CRYPTO_LAST_PATH): SourceKeys(symbol=KeySpec("from", from_request=True))},
)
NEWS_PATH = "/v2/reference/news"
# A list whose rows name no symbol and whose path takes none: no request names a symbol.
NEWS = DatasetSpec(
    summary="A dataset for the tests.",
    name="reference.news",
    fetch_path=NEWS_PATH,
    keys={(R, NEWS_PATH): SourceKeys()},
)
TRADES_PATH = "/v3/trades/{stockTicker}"
TRADES = DatasetSpec(
    summary="A dataset for the tests.",
    name="stocks.trades",
    asset_class=AssetClass.STOCKS,
    history_path=TRADES_PATH,
    time_axis=("timestamp.gte", "timestamp.lt"),
    keys={
        (R, TRADES_PATH): SourceKeys(
            KeySpec("stockTicker", from_request=True), KeySpec("SipTimestamp")
        )
    },
)
SPECS = (DIVIDENDS, LISTED, CRYPTO_LAST, NEWS, TRADES)

TICKERS_PATH = "/v3/reference/tickers"
MONDAY = date(2025, 3, 3)
MIDNIGHT = datetime(2025, 3, 3, 5, tzinfo=UTC)
"""00:00 on Monday in New York."""


@pytest.fixture
def session(
    fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch
) -> Iterator[dm.MassiveSession]:
    monkeypatch.setattr(_session, "CATALOG", Catalog(SPECS))
    # One request at a time, so a script of replies for one path answers them in plan order.
    m = dm.MassiveSession(
        dm.MassiveConfig(api_key="test-key", rest_base_url=fake_rest.url, max_concurrency=1)
    )
    yield m
    m.close()


def _rows(outcome: SymbolOutcome) -> list[dict[str, Any]]:
    assert isinstance(outcome, SymbolRows)
    return cast("list[dict[str, Any]]", dh_arrow.to_arrow(outcome.table).to_pylist())


def _failure(outcome: SymbolOutcome) -> SymbolFailure:
    assert isinstance(outcome, SymbolFailure)
    return outcome


def _dividend(ticker: str, amount: float) -> dict[str, object]:
    return {"ticker": ticker, "cash_amount": amount, "ex_dividend_date": "2025-02-10", "id": ticker}


def _trade(nanos: int) -> dict[str, object]:
    return {"sip_timestamp": nanos, "price": 1.0, "size": 10}


def _fetch(
    m: dm.MassiveSession, spec: DatasetSpec, symbols: list[str], *, adjusted: bool | None = None
) -> Mapping[str, SymbolOutcome]:
    rest = m._core.rest_transport()
    return rest.fetch_by_symbol(spec, Request(adjusted=adjusted), symbols=symbols, call=7)


def _history(
    m: dm.MassiveSession,
    symbols: list[str],
    window: HistoryWindow,
    *,
    adjusted: bool | None = None,
) -> Mapping[str, SymbolOutcome]:
    rest = m._core.rest_transport()
    asked = Request(start=MONDAY, end=MONDAY, adjusted=adjusted)
    return rest.history_by_symbol(TRADES, asked, window, symbols=symbols, call=7)


def _monday() -> HistoryWindow:
    asked = Request(start=MONDAY, end=MONDAY)
    return history_window(TRADES, asked, start=MONDAY, end=MONDAY, now=MIDNIGHT + timedelta(7))


# --- Classes of failure -------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("error", "kind"),
    [
        (MassiveError("rate limited: ...", status=429), FailureKind.TRANSIENT),
        (MassiveError("HTTP 503", status=503), FailureKind.TRANSIENT),
        (MassiveError("connection reset", source=ErrorSource.NETWORK), FailureKind.TRANSIENT),
        # A certificate this machine does not trust fails alike until its settings change.
        (MassiveError("TLS failed", source=ErrorSource.USER), FailureKind.PERMANENT),
        (CutShortError("AAPL's data was cut short", status=404), FailureKind.TRANSIENT),
        # Any other 404, such as one for a request of several symbols or of none (a path
        # Massive retired), would answer the same again.
        (MassiveError("HTTP 404 from GET /v1/retired", status=404), FailureKind.PERMANENT),
        (EntitlementError("not in the plan", status=403), FailureKind.PERMANENT),
        (RowDataError("every row was dropped"), FailureKind.PERMANENT),
        (UnsupportedParameterError("does not take ['adjusted']"), FailureKind.PERMANENT),
        (UnlistedPairError("X:NOPEUSD is not listed"), FailureKind.PERMANENT),
        (MassiveError("unreadable", source=ErrorSource.LIBRARY), FailureKind.PERMANENT),
        (MassiveError("HTTP 400", status=400), FailureKind.PERMANENT),
    ],
)
def test_a_failure_is_transient_only_when_it_is_retryable(
    error: MassiveError, kind: FailureKind
) -> None:
    assert failure_kind(error) is kind


# --- fetch by symbol ----------------------------------------------------------------------------


def test_a_batched_request_splits_its_rows_by_symbol_and_shares_its_failure(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, caplog: pytest.LogCaptureFixture
) -> None:
    # Two symbols a request: AAPL and MSFT share the first, NVDA and TSLA the second.
    first = [_dividend("AAPL", 0.25), _dividend("MSFT", 0.83), _dividend("IBM", 1.67)]
    refusal = Reply({"status": "NOT_AUTHORIZED", "message": "upgrade"}, status=403)
    fake_rest.reply(DIVIDENDS_PATH, Reply(envelope(first)), refusal)
    with caplog.at_level(logging.DEBUG):
        out = _fetch(session, DIVIDENDS, ["TSLA", "MSFT", "AAPL", "NVDA", "AAPL"])
    assert list(out) == ["TSLA", "MSFT", "AAPL", "NVDA"]  # as given, each once
    sent = [query["ticker.any_of"] for _, query in fake_rest.get_queries()]
    assert sent == [["AAPL,MSFT"], ["NVDA,TSLA"]]
    assert [(r["Symbol"], r["CashAmount"]) for r in _rows(out["AAPL"])] == [("AAPL", 0.25)]
    assert [(r["Symbol"], r["CashAmount"]) for r in _rows(out["MSFT"])] == [("MSFT", 0.83)]
    nvda, tsla = _failure(out["NVDA"]), _failure(out["TSLA"])
    assert nvda.error is tsla.error
    assert isinstance(nvda.error, EntitlementError)
    assert nvda.kind is FailureKind.PERMANENT
    # The row of a symbol no one asked for belongs to no symbol's read.
    unowned = [r for r in caplog.records if "name none of those symbols" in r.getMessage()]
    assert [(r.levelno, r.getMessage()) for r in unowned] == [
        (
            logging.WARNING,
            (
                "stocks.dividends: 1 of the 3 rows Massive returned from /stocks/v1/dividends "
                "for 2 [AAPL, MSFT] name none of those symbols, so no symbol's read owns them "
                "and they are left out"
            ),
        )
    ]
    # The shared failure is logged once, at DEBUG: it is expected, and the caller reports it.
    failed = [r for r in caplog.records if "fails this symbol alone" in r.getMessage()]
    assert [r.levelno for r in failed] == [logging.DEBUG, logging.DEBUG]
    assert (
        failed[0]
        .getMessage()
        .startswith(
            "call 7 stocks.dividends TSLA: the read from REST failed (permanent), which fails this "
            "symbol alone: stocks.dividends: the Stocks subscription of your plan does not include "
        )
    )


def test_a_batch_whose_rows_all_belong_to_its_symbols_warns_nothing(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, caplog: pytest.LogCaptureFixture
) -> None:
    fake_rest.reply(DIVIDENDS_PATH, Reply(envelope([_dividend("MSFT", 0.83)])))
    with caplog.at_level(logging.DEBUG, LOGGER):
        out = _fetch(session, DIVIDENDS, ["AAPL", "MSFT"])
    assert [len(_rows(outcome)) for outcome in out.values()] == [0, 1]
    assert "name none" not in caplog.text


def test_an_unowned_row_warns_once_per_dataset_and_path(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, caplog: pytest.LogCaptureFixture
) -> None:
    fake_rest.reply(DIVIDENDS_PATH, Reply(envelope([_dividend("IBM", 1.67)])))
    with caplog.at_level(logging.DEBUG, LOGGER):
        for _ in range(2):
            assert _rows(_fetch(session, DIVIDENDS, ["AAPL", "MSFT"])["AAPL"]) == []
    unowned = [r.levelno for r in caplog.records if "name none" in r.getMessage()]
    assert unowned == [logging.WARNING, logging.DEBUG]


def test_a_row_listing_two_named_tickers_goes_to_each(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    # One request names both tickers; the article that lists both is each one's row.
    fake_rest.reply(
        BENZINGA_NEWS_PATH,
        Reply(
            envelope(
                [
                    {"benzinga_id": 1, "tickers": ["AAPL", "MSFT"]},
                    {"benzinga_id": 2, "tickers": ["MSFT"]},
                ]
            )
        ),
    )
    out = _fetch(session, LISTED, ["AAPL", "MSFT"])
    assert [query["tickers.any_of"] for _, query in fake_rest.get_queries()] == [["AAPL,MSFT"]]
    assert [r["BenzingaId"] for r in _rows(out["AAPL"])] == [1]
    assert [r["BenzingaId"] for r in _rows(out["MSFT"])] == [1, 2]


def test_a_row_listing_none_of_the_named_tickers_keeps_a_null_symbol(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, caplog: pytest.LogCaptureFixture
) -> None:
    # A static read keeps such a row with a null Symbol; a read by symbol keeps it too, with
    # the request's first symbol, and the listing step logs its WARNING.
    fake_rest.reply(
        BENZINGA_NEWS_PATH,
        Reply(
            envelope(
                [
                    {"benzinga_id": 1, "tickers": ["MSFT"]},
                    {"benzinga_id": 2, "tickers": ["IBM"]},
                ]
            )
        ),
    )
    with caplog.at_level(logging.DEBUG):
        out = _fetch(session, LISTED, ["MSFT", "AAPL"])
    assert [(r["BenzingaId"], r["Symbol"]) for r in _rows(out["AAPL"])] == [(2, None)]
    assert [(r["BenzingaId"], r["Symbol"]) for r in _rows(out["MSFT"])] == [(1, "MSFT")]
    listing = [r for r in caplog.records if "list none of the request's tickers" in r.getMessage()]
    assert [r.levelno for r in listing] == [logging.WARNING]
    assert "name none of those symbols" not in caplog.text
    assert "null Symbol; they are kept with AAPL" in caplog.text


def test_rows_of_a_request_that_names_no_symbol_belong_to_none(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, caplog: pytest.LogCaptureFixture
) -> None:
    fake_rest.reply(NEWS_PATH, Reply(envelope([{"id": "n1", "title": "news"}])))
    with caplog.at_level(logging.WARNING, LOGGER):
        out = _fetch(session, NEWS, ["AAPL"])
    assert _rows(out["AAPL"]) == []
    assert "returned from /v2/reference/news for no symbol name none" in caplog.text


def test_a_crypto_pair_massive_does_not_list_fails_alone(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    listed = {"ticker": "X:BTCUSD", "base_currency_symbol": "BTC", "currency_symbol": "USD"}
    fake_rest.reply(TICKERS_PATH, Reply(envelope([listed])), Reply(envelope([])))
    last = {
        "status": "OK",
        "symbol": "BTC-USD",
        "last": {"price": 90000.5, "timestamp": 1741000000000},
    }
    fake_rest.reply("/v1/last/crypto/BTC/USD", Reply(last))
    out = _fetch(session, CRYPTO_LAST, ["X:NOPEUSD", "X:BTCUSD"])
    assert [r["Symbol"] for r in _rows(out["X:BTCUSD"])] == ["X:BTCUSD"]
    nope = _failure(out["X:NOPEUSD"])
    assert isinstance(nope.error, UnlistedPairError)
    assert nope.kind is FailureKind.PERMANENT
    # No request was made for the pair the list does not hold.
    assert [p for p, _ in fake_rest.get_queries() if p != TICKERS_PATH] == [
        "/v1/last/crypto/BTC/USD"
    ]


def test_a_parameter_the_path_does_not_take_fails_its_symbols_before_any_request(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    out = _fetch(session, DIVIDENDS, ["AAPL", "MSFT"], adjusted=False)
    aapl, msft = _failure(out["AAPL"]), _failure(out["MSFT"])
    assert aapl.error is msft.error
    assert isinstance(aapl.error, UnsupportedParameterError)
    assert str(aapl.error) == "stocks.dividends: /stocks/v1/dividends does not take ['adjusted']"
    assert aapl.kind is FailureKind.PERMANENT
    assert fake_rest.get_queries() == []


def test_a_rejected_key_fails_the_whole_read(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply(DIVIDENDS_PATH, Reply({"status": "ERROR"}, status=401))
    with pytest.raises(dm.AuthenticationError):
        _fetch(session, DIVIDENDS, ["AAPL", "MSFT", "NVDA"])


def test_a_rejected_key_while_splitting_a_pair_fails_the_whole_read(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply(TICKERS_PATH, Reply({"status": "ERROR"}, status=401))
    with pytest.raises(dm.AuthenticationError):
        _fetch(session, CRYPTO_LAST, ["X:BTCUSD"])


def test_a_session_closed_during_the_read_fails_it(session: dm.MassiveSession) -> None:
    rest = session._core.rest_transport()
    session.close()
    with pytest.raises(dm.MassiveError, match=r"^MassiveSession #\d+ is closed$"):
        rest.fetch_by_symbol(DIVIDENDS, Request(), symbols=["AAPL"], call=7)


# --- history by symbol --------------------------------------------------------------------------


def test_each_symbol_s_history_ends_in_its_own_outcome(
    *,
    session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # A pause beyond the read's patience: the read gives up on the symbol at once.
    monkeypatch.setattr(rate_limits, "READ_PATIENCE", 300.0)
    start = MIDNIGHT.timestamp() * NS
    fake_rest.reply("/v3/trades/AAPL", Reply(envelope([_trade(int(start) + 1)])))
    # MSFT has no data: HTTP 404 on its first request.
    fake_rest.reply("/v3/trades/NVDA", Reply({"status": "ERROR"}, status=403))
    fake_rest.reply("/v3/trades/TSLA", Reply(b"oops", status=500))
    gone = "/v3/trades/AMZN?cursor=gone"
    fake_rest.reply(gone, Reply(b'{"status":"NOT_FOUND"}', status=404))
    fake_rest.reply(
        "/v3/trades/AMZN",
        Reply(envelope([_trade(int(start) + 2)], next_url=f"{fake_rest.url}{gone}")),
    )
    slow = Reply({"status": "ERROR", "message": "slow down"}, status=429)
    # Last in plan order: the pause holds the whole stocks bucket for the read.
    fake_rest.reply("/v3/trades/WMT", dataclasses.replace(slow, headers={"Retry-After": "700"}))
    symbols = ["AAPL", "MSFT", "NVDA", "TSLA", "AMZN", "WMT"]
    with caplog.at_level(logging.WARNING):
        out = _history(session, symbols, _monday())
    assert [r["Symbol"] for r in _rows(out["AAPL"])] == ["AAPL"]
    assert _rows(out["MSFT"]) == []
    kinds = {symbol: _failure(out[symbol]).kind for symbol in ["NVDA", "TSLA", "AMZN", "WMT"]}
    assert kinds == {
        "NVDA": FailureKind.PERMANENT,
        "TSLA": FailureKind.TRANSIENT,
        "AMZN": FailureKind.TRANSIENT,
        "WMT": FailureKind.TRANSIENT,
    }
    assert isinstance(_failure(out["AMZN"]).error, CutShortError)
    assert "AMZN's data was cut short" in str(_failure(out["AMZN"]).error)
    assert str(_failure(out["WMT"]).error).startswith("rate limited: ")
    # MSFT's missing data warns as history() warns it; no failure warns.
    warned = [r for r in caplog.records if r.name in {LOGGER, REST_LOGGER}]
    assert [r.getMessage() for r in warned if r.levelno >= logging.WARNING] == [
        (
            "stocks.trades: no data for 1 [MSFT]: Massive answered HTTP 404 from "
            "/v3/trades/{stockTicker}, so they are left out"
        )
    ]


def test_a_history_read_made_after_the_clock_moved_keeps_the_call_s_rows(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The call began at 12:00 New York on Monday and reads through then; its symbol is read an
    # hour later, and keeps only the rows before the call's instant.
    call = MIDNIGHT + timedelta(hours=12)
    asked = Request(start=MIDNIGHT, end=None)
    window = history_window(TRADES, asked, start=MIDNIGHT, end=None, now=call)
    assert window.now == call
    monkeypatch.setattr(history_source, "_now", lambda: call + timedelta(hours=1))
    # A window built by the moved clock would keep the later row.
    later = history_window(TRADES, asked, start=MIDNIGHT, end=None)
    assert later.times is not None
    assert later.times.end == call + timedelta(hours=1)
    before, after = int(call.timestamp()) * NS - 1, int(call.timestamp()) * NS + 1
    fake_rest.reply("/v3/trades/AAPL", Reply(envelope([_trade(before), _trade(after)])))
    rest = session._core.rest_transport()
    out = rest.history_by_symbol(TRADES, asked, window, symbols=["AAPL"], call=7)
    assert len(_rows(out["AAPL"])) == 1
    [(_, query)] = fake_rest.get_queries(one_value=True)
    assert query["timestamp.lt"] == str(int(call.timestamp()) * NS)


def test_an_empty_window_reads_nothing(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    asked = Request(start=MIDNIGHT, end=MIDNIGHT)
    window = history_window(TRADES, asked, start=MIDNIGHT, end=MIDNIGHT, now=MIDNIGHT)
    assert window.empty
    rest = session._core.rest_transport()
    out = rest.history_by_symbol(TRADES, asked, window, symbols=["AAPL", "MSFT"], call=7)
    assert [_rows(outcome) for outcome in out.values()] == [[], []]
    assert fake_rest.get_queries() == []


def test_a_history_parameter_the_path_does_not_take_fails_every_symbol(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    out = _history(session, ["AAPL", "MSFT"], _monday(), adjusted=False)
    assert {_failure(o).kind for o in out.values()} == {FailureKind.PERMANENT}
    assert isinstance(_failure(out["AAPL"]).error, UnsupportedParameterError)
    assert fake_rest.get_queries() == []


# --- Flat files ---------------------------------------------------------------------------------

FLAT_TRADES = _session.CATALOG.by_name["stocks.trades"]
"""A dataset with flat files, from the real catalog."""


def _flat_rows(m: dm.MassiveSession, symbols: list[str | None]) -> Table:
    schema = m._core.dataset_schema(FLAT_TRADES)
    rows = pa.Table.from_pylist([{"Symbol": s} for s in symbols], schema=schema.arrow_schema())
    with entered(m._core.exec_ctx):
        return dataset_table(rows, schema.date_columns)


def _flat_window() -> HistoryWindow:
    asked = Request(start=MONDAY, end=MONDAY)
    return history_window(FLAT_TRADES, asked, start=MONDAY, end=MONDAY, now=MIDNIGHT + timedelta(7))


def test_a_split_by_symbol_warns_about_rows_no_symbol_owns(
    massive_session: dm.MassiveSession, caplog: pytest.LogCaptureFixture
) -> None:
    table = _flat_rows(massive_session, ["AAPL", "MSFT", "AAPL", "IBM", None])
    with caplog.at_level(logging.WARNING, LOGGER):
        out = split_by_symbol(
            massive_session._core,
            FLAT_TRADES,
            table,
            symbols=["NVDA", "AAPL", "MSFT"],
            source="flat files",
        )
    assert list(out) == ["NVDA", "AAPL", "MSFT"]
    assert [part.size for part in out.values()] == [0, 2, 1]
    assert caplog.messages == [
        (
            "stocks.trades: 2 of the 5 rows read from flat files for 3 [NVDA, AAPL, MSFT] name "
            "none of those symbols (or no symbol), so no symbol's read owns them and they are "
            "left out"
        )
    ]


def test_a_split_that_keeps_every_row_warns_nothing(
    massive_session: dm.MassiveSession, caplog: pytest.LogCaptureFixture
) -> None:
    table = _flat_rows(massive_session, ["AAPL"])
    with caplog.at_level(logging.WARNING, LOGGER):
        split_by_symbol(
            massive_session._core, FLAT_TRADES, table, symbols=["AAPL"], source="flat files"
        )
    assert caplog.messages == []


def test_a_table_without_the_dataset_s_columns_is_a_library_defect(
    massive_session: dm.MassiveSession, caplog: pytest.LogCaptureFixture
) -> None:
    # Built from Arrow, not formulas: a formula's query scope would capture this frame, and
    # with it the session.
    schema = massive_session._core.dataset_schema(FLAT_TRADES).arrow_schema()
    columns = {field.name: pa.nulls(1, field.type) for field in schema if field.name != "Price"}
    columns |= {"Symbol": pa.array(["AAPL"]), "Exchange": pa.array(["NYSE"])}
    columns["Extra"] = pa.array([1])
    with entered(massive_session._core.exec_ctx):
        broken = dh_arrow.to_table(pa.table(columns))
    with caplog.at_level(logging.ERROR, LOGGER):
        out = symbol_outcomes(
            massive_session._core,
            FLAT_TRADES,
            symbols=["AAPL"],
            tables={"AAPL": broken},
            failures={},
            call=7,
            source="flat files",
        )
    failure = _failure(out["AAPL"])
    assert failure.kind is FailureKind.PERMANENT
    assert failure.error.source is ErrorSource.LIBRARY
    assert str(failure.error) == (
        "stocks.trades: AAPL's rows from flat files lack Price; have Extra, which the dataset "
        f"does not; have Exchange as java.lang.String, not long. {LIBRARY_BUG}"
    )
    assert [r.levelno for r in caplog.records] == [logging.ERROR]


# --- fetch by value -----------------------------------------------------------------------------


PRODUCTS_PATH = "/futures/v1/products"


def _by_value(
    m: dm.MassiveSession, codes: list[str], *, adjusted: bool | None = None
) -> Mapping[str, SymbolOutcome]:
    # Reads futures products for each code of a table of product codes.
    with entered(m._core.exec_ctx):
        table = new_table([string_col("ProductCode", codes)])
    values = ValueTable(
        "product_code", "product_code", "ProductCode", underlying=False, table=table
    )
    request = Request(adjusted=adjusted, value_table=values)
    rest = m._core.rest_transport()
    return rest.fetch_by_value(futures.PRODUCTS, request, values=codes, call=7)


def test_each_value_is_read_alone_and_fails_alone(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    refusal = Reply({"status": "NOT_AUTHORIZED", "message": "upgrade"}, status=403)
    # One value after another, in the order given: NQ, then ES.
    es = Reply(envelope([{"product_code": "ES", "name": "E-mini S&P 500"}]))
    fake_rest.reply(PRODUCTS_PATH, refusal, es)
    out = _by_value(rest_session, ["NQ", "ES", "NQ"])
    assert list(out) == ["NQ", "ES"]  # as given, each once
    assert [query["product_code"] for _, query in fake_rest.get_queries()] == [["NQ"], ["ES"]]
    assert [r["ProductCode"] for r in _rows(out["ES"])] == ["ES"]
    assert isinstance(_failure(out["NQ"]).error, EntitlementError)


def test_a_rejected_key_stops_a_read_by_value(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply(PRODUCTS_PATH, Reply({"status": "ERROR"}, status=401))
    with pytest.raises(dm.AuthenticationError):
        _by_value(rest_session, ["ES", "NQ"])
    assert len(fake_rest.get_queries()) == 1  # no value is read after the refusal


def test_a_parameter_the_path_does_not_take_fails_each_value_before_any_request(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    out = _by_value(rest_session, ["ES"], adjusted=False)
    error = _failure(out["ES"]).error
    assert isinstance(error, UnsupportedParameterError)
    assert str(error) == "futures.products: /futures/v1/products does not take ['adjusted']"
    assert fake_rest.get_queries() == []
