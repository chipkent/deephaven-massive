"""Tests for reads by symbol: each symbol's rows or its own failure, through the transports.

The REST reads run against the fake REST server; the flat-file read is replaced by a scripted
table or failure, since only the split of its rows and failures by symbol is this module's.
"""

from __future__ import annotations

import dataclasses
import logging
import re
from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

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
from deephaven_massive._error_policy import resolve_policy
from deephaven_massive._errors import (
    LIBRARY_BUG,
    AuthenticationError,
    CutShortError,
    EntitlementError,
    ErrorSource,
    FeedRefusalError,
    MassiveError,
    RowDataError,
    SessionClosedError,
    UnavailableSymbolError,
    session_closed,
)
from deephaven_massive._ingest import rate_limits, rest
from deephaven_massive._ingest.symbol_reads import (
    AttributedRows,
    RequestRows,
    UnsupportedParameterError,
    asked_column,
    attribute_units,
    fails_followed_read,
    fetch_units,
    split_by_symbol,
    symbol_outcomes,
)
from deephaven_massive._massive import Transport
from deephaven_massive._tables import problem_reporter
from deephaven_massive._tables.columns import dataset_table
from deephaven_massive._tables.execution_context import entered
from deephaven_massive._tables.problem_events import ProblemAction, ProblemCause, ProblemEvent
from deephaven_massive._tables.read_tally import ReadTally
from deephaven_massive._tables.transports import SymbolFailure, SymbolRows
from deephaven_massive._tables.withheld import SYMBOL_SENT, Withheld
from deephaven_massive.namespaces import _futures as futures
from deephaven_massive.namespaces import _options as options
from tests._fakes.rest import FakeRestServer, Reply, envelope

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping, Sequence

    from deephaven.table import Table

    from deephaven_massive._datasets.history_source import HistoryWindow
    from deephaven_massive._tables.transports import SymbolOutcome

LOGGER = "deephaven_massive._ingest.symbol_reads"
REST_LOGGER = "deephaven_massive._ingest.rest"


def alarms(caplog: pytest.LogCaptureFixture) -> list[str]:
    # The problem reporter's WARNINGs: the first problem of each cause on each read.
    return [
        r.getMessage()
        for r in caplog.records
        if r.name == problem_reporter.__name__ and r.levelno == logging.WARNING
    ]


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
PREV_PATH = "/v2/aggs/ticker/{stocksTicker}/prev"
# A request for one symbol by its own path whose rows echo the ticker: Symbol comes from the row.
PREV = DatasetSpec(
    summary="A dataset for the tests.",
    name="stocks.previous_day_bar",
    asset_class=AssetClass.STOCKS,
    fetch_path=PREV_PATH,
    per_symbol=True,
    keys={(R, PREV_PATH): SourceKeys(symbol=KeySpec("Ticker"))},
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
RELATED_PATH = "/v1/related-companies/{ticker}"
# A request for one symbol by its own path, whose rows name the related company, not the
# company asked about.
RELATED = DatasetSpec(
    summary="A dataset for the tests.",
    name="reference.related_companies",
    asset_class=AssetClass.STOCKS,
    fetch_path=RELATED_PATH,
    keys={
        (R, RELATED_PATH): SourceKeys(
            symbol=KeySpec("Ticker"), fanout=KeySpec("ticker", from_request=True)
        )
    },
)
RATINGS_PATH = "/benzinga/v1/ratings"
# A list parameter whose rows may name no ticker: such a row keeps a null Symbol.
RATINGS = DatasetSpec(
    summary="A dataset for the tests.",
    name="partners.benzinga.ratings",
    fetch_path=RATINGS_PATH,
    symbol_param="ticker.any_of",
    keys={(R, RATINGS_PATH): SourceKeys(symbol=KeySpec("Ticker", required=False))},
    poll_keys=("BenzingaId",),
)
INDEX_PATH = "/v3/snapshot/indices"
# A list parameter on indices, which Massive may write bare (`NDX`) and the table names `I:NDX`.
INDEX = DatasetSpec(
    summary="A dataset for the tests.",
    name="indices.snapshots",
    asset_class=AssetClass.INDICES,
    fetch_path=INDEX_PATH,
    symbol_param="ticker.any_of",
    keys={(R, INDEX_PATH): SourceKeys(symbol=KeySpec("Ticker"))},
)
SPECS = (
    DIVIDENDS,
    LISTED,
    CRYPTO_LAST,
    PREV,
    TRADES,
    RELATED,
    RATINGS,
    INDEX,
    # The datasets whose index lookup prefixes a column after the page is read: the open
    # interest by contract (`Underlying`) and by underlying (`Symbol`).
    options.OPEN_INTEREST,
    options.OPEN_INTEREST_BY_UNDERLYING,
)
BY_ROOT_PATH = "/options/v1/open-interest"
BY_UNDERLYING_PATH = "/options/v1/open-interest/underlying"
CONTRACTS_LIST_PATH = "/v3/reference/options/contracts"

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
    rows = dh_arrow.to_arrow(outcome.table).to_pylist()
    assert isinstance(rows, list)
    return rows


def _failure(outcome: SymbolOutcome) -> SymbolFailure:
    assert isinstance(outcome, SymbolFailure)
    return outcome


def _dividend(ticker: str, amount: float) -> dict[str, object]:
    return {"ticker": ticker, "cash_amount": amount, "ex_dividend_date": "2025-02-10", "id": ticker}


def _trade(nanos: int) -> dict[str, object]:
    return {"sip_timestamp": nanos, "price": 1.0, "size": 10}


def _contract_open_interest(contract: str, open_interest: object) -> dict[str, object]:
    # One row of the open interest by contract; `root` names the contract's option root.
    root = contract[2:].rstrip("0123456789CP")
    return {
        "ticker": contract,
        "root": root,
        "date": "2026-01-23",
        "underlying_ticker": root,
        "open_interest": open_interest,
    }


def _underlying_open_interest(underlying: str, open_interest: object) -> dict[str, object]:
    # One row of the open interest by underlying: Massive writes an index bare (`SPX`).
    return {
        "underlying_ticker": underlying,
        "date": "2026-01-23",
        "open_interest": open_interest,
        "open_interest_calls": 1,
        "open_interest_puts": 2,
    }


def _serve_index_list(server: FakeRestServer, *names: str) -> None:
    # Massive's index ticker list, which the index lookup reads once per session.
    rows = [{"ticker": f"I:{name}", "market": "indices"} for name in names]
    server.reply(TICKERS_PATH, Reply(envelope(rows)))


def _fetch(
    m: dm.MassiveSession,
    spec: DatasetSpec,
    symbols: list[str],
    *,
    adjusted: bool | None = None,
    unrequested: dm.OnError | None = None,
) -> Mapping[str, SymbolOutcome]:
    rest = m._core.rest_transport()
    policy = resolve_policy(
        dm.ErrorPolicy(unrequested_symbols=unrequested), m._core.config.on_error
    )
    asked = Request(adjusted=adjusted, on_error=policy)
    return rest.fetch_by_symbol(spec, asked, symbols=symbols, call=7)


def _problems(m: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch) -> list[ProblemEvent]:
    # The problems the session's reads report, in order; they still reach its reporter, which
    # logs the alarm, and m.problems().
    events: list[ProblemEvent] = []
    report = m._core.problems

    def recorded(found: Sequence[ProblemEvent]) -> None:
        events.extend(found)
        report(found)

    monkeypatch.setattr(m._core, "problems", recorded)
    return events


def _unrequested(
    symbol: str | None,
    rows: int,
    *,
    asked: str,
    setting: dm.OnError = dm.OnError.LOG,
    spec: DatasetSpec = DIVIDENDS,
    where: str = "REST GET /stocks/v1/dividends",
    field: str = "Symbol",
    detail: str | None = None,
) -> ProblemEvent:
    # The event of a request's rows of a symbol it did not ask for, with `at` zeroed.
    named = "no symbol" if symbol is None else symbol
    return ProblemEvent(
        cause=ProblemCause.UNREQUESTED_SYMBOLS,
        action=ProblemAction.WITHHELD,
        dataset=spec.name,
        stream_id=7,
        where=where,
        symbol=symbol,
        field=field,
        rows=rows,
        detail=f"Massive answered the request for {asked} with rows of {named}, which the "
        "read did not ask for, so they are left out"
        if detail is None
        else detail,
        at=0,
        policy=setting,
    )


def _zeroed(events: Sequence[ProblemEvent]) -> list[ProblemEvent]:
    return [dataclasses.replace(e, at=0) for e in events]


def _history(
    m: dm.MassiveSession,
    symbols: list[str],
    window: HistoryWindow,
    *,
    adjusted: bool | None = None,
) -> Mapping[str, SymbolOutcome]:
    rest = m._core.rest_transport()
    asked = Request(start=MONDAY, end=MONDAY, adjusted=adjusted, on_error=m._core.policy)
    return rest.history_by_symbol(TRADES, asked, window, symbols=symbols, call=7, tally=ReadTally())


def _monday() -> HistoryWindow:
    asked = Request(start=MONDAY, end=MONDAY)
    return history_window(TRADES, asked, start=MONDAY, end=MONDAY, now=MIDNIGHT + timedelta(7))


# --- Classes of failure -------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("error", "retryable"),
    [
        (MassiveError("rate limited: ...", status=429), True),
        (MassiveError("HTTP 503", status=503), True),
        (MassiveError("connection reset", source=ErrorSource.NETWORK), True),
        # A certificate this machine does not trust fails alike until its settings change.
        (MassiveError("TLS failed", source=ErrorSource.USER), False),
        (CutShortError("AAPL's data was cut short", status=404), True),
        # S3's RequestTimeout on a flat-file scan: an HTTP 400 that means try again.
        (
            MassiveError("HTTP 400 (RequestTimeout)", status=400, is_retryable=True),
            True,
        ),
        # Any other 404, such as one for a request of several symbols or of none (a path
        # Massive retired), would answer the same again.
        (MassiveError("HTTP 404 from GET /v1/retired", status=404), False),
        (EntitlementError("not in the plan", status=403), False),
        (RowDataError("every row was dropped"), False),
        (UnsupportedParameterError("does not take ['adjusted']"), False),
        (UnavailableSymbolError("Massive has no data for X:NOPEUSD"), False),
        (MassiveError("unreadable", source=ErrorSource.LIBRARY), False),
        (MassiveError("HTTP 400", status=400), False),
    ],
)
def test_a_failure_is_retryable_only_when_its_cause_may_pass(
    error: MassiveError, *, retryable: bool
) -> None:
    assert error.is_retryable is retryable


@pytest.mark.parametrize(
    ("error", "whole"),
    [
        (AuthenticationError("the key is refused", status=401), True),
        (SessionClosedError(session_closed()), True),
        (EntitlementError("refused", status=403), False),
        (FeedRefusalError("delayed", source=ErrorSource.USER), False),
        (MassiveError("HTTP 503", status=503), False),
        (MassiveError("connection reset", source=ErrorSource.NETWORK), False),
        (CutShortError("cut short", status=404), False),
        (RowDataError("a row the library cannot read"), False),
        (UnavailableSymbolError("no data"), False),
        (MassiveError("a defect", source=ErrorSource.LIBRARY), False),
    ],
    ids=[
        "a rejected key",
        "a closed session",
        "a refusal",
        "a feed's refusal",
        "a server error",
        "a network failure",
        "a symbol's data cut short",
        "a row the library cannot read",
        "a symbol with no data under RAISE",
        "a defect",
    ],
)
def test_a_followed_read_fails_whole_only_for_what_fails_every_symbol_alike(
    error: MassiveError, *, whole: bool
) -> None:
    assert fails_followed_read(error) is whole


def test_attribution_gives_each_request_s_rows_failure_or_no_data_to_its_symbols(
    session: dm.MassiveSession,
) -> None:
    transport = session._core.rest_transport()
    assert isinstance(transport, rest.RestEndpoints)
    reader = transport._reader("call-7", session._core.policy, waiter=rate_limits.Waiter.CALL)
    symbols = ["AAPL", "MSFT", "NVDA"]
    units, _ = fetch_units(reader, DIVIDENDS, Request(), symbols=symbols, cancel=None)
    assert [call.symbols for _, call in units] == [("AAPL", "MSFT"), ("NVDA",)]
    schema = session._core.dataset_schema(DIVIDENDS).arrow_schema()
    sent = pa.Table.from_pylist(
        [{"Symbol": "AAPL", "CashAmount": 0.25, SYMBOL_SENT: "AAPL"}],
        schema=schema.append(pa.field(SYMBOL_SENT, pa.string())),
    )
    answered = RequestRows(sent, Withheld(pa.table({})), "GET /stocks/v1/dividends")
    error = MassiveError("HTTP 503", status=503)
    tally = ReadTally()
    # The first request answered with AAPL's row; NVDA's had no data.
    out = attribute_units(
        reader, DIVIDENDS, units, results=[answered, None], symbols=symbols, tally=tally
    )
    assert out.failures == {}
    assert list(out.pieces) == symbols
    [aapl] = out.pieces["AAPL"]
    assert aapl.equals(sent.drop_columns([SYMBOL_SENT]))
    [msft] = out.pieces["MSFT"]  # named by the answered request, which gave it no row
    assert msft.num_rows == 0
    assert out.pieces["NVDA"] == []  # named only by the request with no data
    # The first request failed: its failure is both its symbols'.
    out = attribute_units(
        reader, DIVIDENDS, units, results=[error, None], symbols=symbols, tally=ReadTally()
    )
    assert out == AttributedRows(
        {"AAPL": [], "MSFT": [], "NVDA": []}, {"AAPL": error, "MSFT": error}
    )
    assert out.failures["AAPL"] is error  # the one error object, shared by the request's symbols


# --- fetch by symbol ----------------------------------------------------------------------------


def test_a_batched_request_splits_its_rows_by_symbol_and_searches_its_refusal(
    *,
    session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events = _problems(session, monkeypatch)
    # Two symbols a request: AAPL and MSFT share the first, NVDA and TSLA the second.
    first = [_dividend("AAPL", 0.25), _dividend("MSFT", 0.83), _dividend("IBM", 1.67)]
    refusal = Reply({"status": "NOT_AUTHORIZED", "message": "upgrade"}, status=403)
    fake_rest.reply(DIVIDENDS_PATH, Reply(envelope(first)), refusal)
    with caplog.at_level(logging.DEBUG):
        out = _fetch(session, DIVIDENDS, ["TSLA", "MSFT", "AAPL", "NVDA", "AAPL"])
    assert list(out) == ["TSLA", "MSFT", "AAPL", "NVDA"]  # as given, each once
    sent = [query["ticker.any_of"] for _, query in fake_rest.get_queries()]
    # Massive refused NVDA and TSLA together, so each is sent alone to learn whose refusal it
    # is; both are refused, so the search ends there.
    assert sorted(sent) == [["AAPL,MSFT"], ["NVDA"], ["NVDA,TSLA"], ["TSLA"]]
    assert [(r["Symbol"], r["CashAmount"]) for r in _rows(out["AAPL"])] == [("AAPL", 0.25)]
    assert [(r["Symbol"], r["CashAmount"]) for r in _rows(out["MSFT"])] == [("MSFT", 0.83)]
    nvda, tsla = _failure(out["NVDA"]), _failure(out["TSLA"])
    for failure in (nvda, tsla):
        assert isinstance(failure.error, EntitlementError)
        assert not failure.error.is_retryable
    # The row of a symbol no one asked for belongs to no symbol's read: it is the read's
    # problem, alarmed once.
    assert _zeroed(events) == [_unrequested("IBM", 1, asked="AAPL and MSFT")]
    assert alarms(caplog) == [
        (
            "stocks.dividends table 7: unrequested_symbols: 1 row of IBM, Massive answered the "
            "request for AAPL and MSFT with rows of IBM, which the read did not ask for, so "
            "they are left out (at REST GET /stocks/v1/dividends). m.problems() lists it and "
            "every later unrequested_symbols problem of this table, which the log shows at "
            "DEBUG only."
        )
    ]
    # Each refusal is logged at DEBUG: it is expected, and the caller reports it.
    failed = [r for r in caplog.records if "failed alone" in r.getMessage()]
    assert [r.levelno for r in failed] == [logging.DEBUG, logging.DEBUG]
    assert failed[0].getMessage() == (
        "call 7 stocks.dividends TSLA failed alone, in its read from REST: Massive refused this "
        "read for the Stocks subscription. Massive answered GET /stocks/v1/dividends?limit=5000&"
        'ticker.any_of=TSLA on 127.0.0.1 with HTTP 403: "upgrade".'
    )


def test_a_batch_whose_rows_all_belong_to_its_symbols_reports_no_problem(
    *,
    session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events = _problems(session, monkeypatch)
    fake_rest.reply(DIVIDENDS_PATH, Reply(envelope([_dividend("MSFT", 0.83)])))
    with caplog.at_level(logging.DEBUG):
        out = _fetch(session, DIVIDENDS, ["AAPL", "MSFT"])
    assert [len(_rows(outcome)) for outcome in out.values()] == [0, 1]
    assert events == []
    assert alarms(caplog) == []


def test_an_unrequested_row_alarms_once_per_table(
    *,
    session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events = _problems(session, monkeypatch)
    fake_rest.reply(DIVIDENDS_PATH, Reply(envelope([_dividend("IBM", 1.67)])))
    with caplog.at_level(logging.DEBUG):
        for _ in range(2):
            assert _rows(_fetch(session, DIVIDENDS, ["AAPL", "MSFT"])["AAPL"]) == []
    assert [e.symbol for e in events] == ["IBM", "IBM"]
    assert len(alarms(caplog)) == 1


def test_a_row_of_another_symbol_is_no_row_of_the_read_s_symbols(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The read's one row of AAPL is withheld, and the IBM row beside it is nobody's: the read
    # could read none of its own rows whole, so AAPL fails as it would with the IBM row absent.
    events = _problems(session, monkeypatch)
    rows = [{**_dividend("AAPL", 0.25), "cash_amount": "a lot"}, _dividend("IBM", 1.67)]
    fake_rest.reply(DIVIDENDS_PATH, Reply(envelope(rows)))
    out = _fetch(session, DIVIDENDS, ["AAPL"])
    failed = _failure(out["AAPL"])
    assert isinstance(failed.error, RowDataError)
    assert str(failed.error) == (
        "stocks.dividends: the library could not read the 1 row Massive sent for AAPL from "
        "GET /stocks/v1/dividends whole, so it withheld it. /stocks/v1/dividends: row 0: "
        "CashAmount (wire key 'cash_amount') expects double, got \"a lot\". m.problems() counts "
        "the withheld rows by cause and symbol."
    )
    assert [(e.cause, e.symbol, e.rows) for e in events] == [
        (ProblemCause.UNCONVERTIBLE_VALUES, "AAPL", 1),
        (ProblemCause.UNREQUESTED_SYMBOLS, "IBM", 1),
    ]


def test_a_request_answered_with_only_another_symbol_s_rows_fails_no_symbol(
    *,
    session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Massive answers AAPL's request with two IBM rows, one of them unreadable. Neither is
    # AAPL's, withheld or not: AAPL gets no rows and no failure. The withheld row stays an
    # unconvertible_values problem under IBM, and the kept one an unrequested_symbols problem.
    events = _problems(session, monkeypatch)
    rows = [{**_dividend("IBM", 1.67), "cash_amount": "a lot"}, _dividend("IBM", 1.68)]
    fake_rest.reply(DIVIDENDS_PATH, Reply(envelope(rows)))
    with caplog.at_level(logging.DEBUG):
        out = _fetch(session, DIVIDENDS, ["AAPL"])
    assert _rows(out["AAPL"]) == []
    assert [(e.cause, e.symbol, e.rows) for e in events] == [
        (ProblemCause.UNCONVERTIBLE_VALUES, "IBM", 1),
        (ProblemCause.UNREQUESTED_SYMBOLS, "IBM", 1),
    ]
    assert len(alarms(caplog)) == 2


def test_a_withheld_row_of_another_symbol_leaves_the_read_s_count_and_its_reason(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Over the request's two pages, IBM's rows are withheld first and AAPL's one row last:
    # AAPL fails for its own row alone, and the failure quotes that row's reason, not IBM's.
    events = _problems(session, monkeypatch)
    second = f"{DIVIDENDS_PATH}?cursor=2"
    first_page = [{**_dividend("IBM", 1.67), "cash_amount": "a lot"}]
    second_page = [
        {**_dividend("IBM", 1.68), "cash_amount": "a lot"},
        {**_dividend("AAPL", 0.25), "cash_amount": "plenty"},
    ]
    fake_rest.reply(
        DIVIDENDS_PATH, Reply(envelope(first_page, next_url=f"{fake_rest.url}{second}"))
    )
    fake_rest.reply(second, Reply(envelope(second_page)))
    out = _fetch(session, DIVIDENDS, ["AAPL"])
    failed = _failure(out["AAPL"])
    assert isinstance(failed.error, RowDataError)
    assert str(failed.error) == (
        "stocks.dividends: the library could not read the 1 row Massive sent for AAPL from "
        "GET /stocks/v1/dividends whole, so it withheld it. /stocks/v1/dividends: row 1: "
        "CashAmount (wire key 'cash_amount') expects double, got \"plenty\". m.problems() counts "
        "the withheld rows by cause and symbol."
    )
    assert [(e.cause, e.symbol, e.rows) for e in events] == [
        (ProblemCause.UNCONVERTIBLE_VALUES, "IBM", 1),
        (ProblemCause.UNCONVERTIBLE_VALUES, "IBM", 1),
        (ProblemCause.UNCONVERTIBLE_VALUES, "AAPL", 1),
    ]


def test_a_withheld_row_of_another_symbol_s_request_is_not_the_read_s(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    # By a symbol's own path, AAPL's request is answered with an unreadable IBM row and MSFT's
    # with an unreadable row of its own: MSFT fails for its own row, naming its request, and
    # AAPL, which Massive sent no row of, gets no rows and no failure.
    fake_rest.reply("/v2/aggs/ticker/AAPL/prev", Reply(envelope([{"T": "IBM", "c": "a lot"}])))
    fake_rest.reply("/v2/aggs/ticker/MSFT/prev", Reply(envelope([{"T": "MSFT", "c": "a lot"}])))
    out = _fetch(session, PREV, ["AAPL", "MSFT"])
    assert str(_failure(out["MSFT"]).error) == (
        "stocks.previous_day_bar: the library could not read the 1 row Massive sent for MSFT "
        "from GET /v2/aggs/ticker/MSFT/prev whole, so it withheld it. "
        "/v2/aggs/ticker/{stocksTicker}/prev: row 0: Close (wire key 'c') expects double, got "
        '"a lot". m.problems() counts the withheld rows by cause and symbol.'
    )
    assert _rows(out["AAPL"]) == []


def test_a_symbol_with_no_rows_beside_one_wholly_withheld_fails_only_the_withheld_one(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    # One list request for AAPL and MSFT: Massive answers with one MSFT row the library can't
    # read whole, and nothing for AAPL. MSFT fails for it; AAPL gets no rows and no failure.
    events = _problems(session, monkeypatch)
    rows = [{**_dividend("MSFT", 0.83), "cash_amount": "a lot"}]
    fake_rest.reply(DIVIDENDS_PATH, Reply(envelope(rows)))
    out = _fetch(session, DIVIDENDS, ["AAPL", "MSFT"])
    assert str(_failure(out["MSFT"]).error) == (
        "stocks.dividends: the library could not read the 1 row Massive sent for MSFT from "
        "GET /stocks/v1/dividends whole, so it withheld it. /stocks/v1/dividends: row 0: "
        "CashAmount (wire key 'cash_amount') expects double, got \"a lot\". m.problems() counts "
        "the withheld rows by cause and symbol."
    )
    assert _rows(out["AAPL"]) == []
    assert [(e.cause, e.symbol, e.rows) for e in events] == [
        (ProblemCause.UNCONVERTIBLE_VALUES, "MSFT", 1)
    ]


def test_a_request_whose_only_row_has_an_unreadable_symbol_fails_its_symbols(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    # The list request's only row names no symbol the library can read: it could be AAPL's or
    # MSFT's, and the request kept no row, so both fail. Beside a row the request kept, the
    # same row fails no one.
    unread = {**_dividend("AAPL", 0.25), "ticker": 7}
    fake_rest.reply(
        DIVIDENDS_PATH,
        Reply(envelope([unread])),
        Reply(envelope([unread, _dividend("MSFT", 0.83)])),
    )
    out = _fetch(session, DIVIDENDS, ["AAPL", "MSFT"])
    for symbol in ("AAPL", "MSFT"):
        assert str(_failure(out[symbol]).error).startswith(
            "stocks.dividends: the library could not read the 1 row Massive sent for AAPL and "
            "MSFT from GET /stocks/v1/dividends whole, so it withheld it. "
        )
    out = _fetch(session, DIVIDENDS, ["AAPL", "MSFT"])
    assert (len(_rows(out["AAPL"])), len(_rows(out["MSFT"]))) == (0, 1)


def test_a_withheld_row_of_another_contract_is_not_the_read_s_by_root(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The open interest by contract gives `Underlying` its I: by a lookup after the page is
    # read, but the asked column is `Symbol`, the contract, which the page carries canonical:
    # an unreadable row of another contract is counted out, and the asked contract gets no
    # rows and no failure.
    events = _problems(session, monkeypatch)
    asked, other = "O:AAPL260116C00200000", "O:MSFT260116C00300000"
    fake_rest.reply(BY_ROOT_PATH, Reply(envelope([_contract_open_interest(other, "a lot")])))
    out = _fetch(session, options.OPEN_INTEREST, [asked])
    assert _rows(out[asked]) == []
    assert [(e.cause, e.symbol, e.rows) for e in events] == [
        (ProblemCause.UNCONVERTIBLE_VALUES, other, 1)
    ]


def test_a_withheld_row_of_a_bare_underlying_is_its_asked_index_s(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The open interest by underlying gives `Symbol` its I: by a lookup after the page is
    # read, so a withheld row's bare SPX goes to the asked I:SPX, which fails for it, and
    # QQQ's is another underlying's, counted out. Each bare ticker keeps its own problem.
    events = _problems(session, monkeypatch)
    page = [
        _underlying_open_interest("SPX", "a lot"),
        _underlying_open_interest("QQQ", "plenty"),
    ]
    fake_rest.reply(BY_UNDERLYING_PATH, Reply(envelope(page)))
    out = _fetch(session, options.OPEN_INTEREST_BY_UNDERLYING, ["I:SPX"])
    failed = _failure(out["I:SPX"])
    assert isinstance(failed.error, RowDataError)
    assert str(failed.error).startswith(
        "options.open_interest_by_underlying: the library could not read the 1 row Massive "
        "sent for I:SPX from GET /options/v1/open-interest/underlying whole, so it withheld "
        "it. "
    )
    assert [(e.cause, e.symbol, e.rows) for e in events] == [
        (ProblemCause.UNCONVERTIBLE_VALUES, "SPX", 1),
        (ProblemCause.UNCONVERTIBLE_VALUES, "QQQ", 1),
    ]


def test_a_withheld_row_of_another_bare_underlying_fails_no_one_by_underlying(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    # SPY's request is answered with one SPX row whose open interest is "a lot": the row is
    # withheld under SPX, before the lookup, and is another underlying's, so SPY gets no rows
    # and no failure.
    events = _problems(session, monkeypatch)
    fake_rest.reply(
        BY_UNDERLYING_PATH, Reply(envelope([_underlying_open_interest("SPX", "a lot")]))
    )
    out = _fetch(session, options.OPEN_INTEREST_BY_UNDERLYING, ["SPY"])
    assert _rows(out["SPY"]) == []
    assert [(e.cause, e.symbol, e.rows) for e in events] == [
        (ProblemCause.UNCONVERTIBLE_VALUES, "SPX", 1)
    ]


def test_a_row_the_index_lookup_withholds_of_another_underlying_fails_no_one(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Massive answers SPY's request with a row of SPX, whose contracts list can't say whether
    # it is an index: the lookup withholds the row, and its bare SPX is no spelling of SPY, so
    # SPY gets no rows and no failure. The row keeps its problem.
    events = _problems(session, monkeypatch)
    fake_rest.reply(BY_UNDERLYING_PATH, Reply(envelope([_underlying_open_interest("SPX", 3)])))
    _serve_index_list(fake_rest, "SPX")
    fake_rest.reply(CONTRACTS_LIST_PATH, Reply(envelope([])))
    out = _fetch(session, options.OPEN_INTEREST_BY_UNDERLYING, ["SPY"])
    assert _rows(out["SPY"]) == []
    assert [(e.cause, e.symbol, e.rows) for e in events] == [
        (ProblemCause.UNREADABLE_SYMBOLS, "SPX", 1)
    ]


def test_a_row_the_root_lookup_withholds_of_another_contract_fails_no_one(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The open interest by contract: Massive answers an AAPL contract's request with a row of
    # an SPX contract that Massive's contracts reference doesn't hold, so the lookup can't say
    # whether its root is an index's and withholds the row. Its contract is another symbol,
    # so the asked contract gets no rows and no failure.
    events = _problems(session, monkeypatch)
    asked, other = "O:AAPL260116C00200000", "O:SPX260116C05000000"
    fake_rest.reply(BY_ROOT_PATH, Reply(envelope([_contract_open_interest(other, 3)])))
    _serve_index_list(fake_rest, "SPX")
    fake_rest.reply(f"/v3/reference/options/contracts/{other}", Reply(status=404))
    out = _fetch(session, options.OPEN_INTEREST, [asked])
    assert _rows(out[asked]) == []
    assert [(e.cause, e.symbol, e.rows) for e in events] == [
        (ProblemCause.UNREADABLE_SYMBOLS, "SPX", 1)
    ]


def test_a_row_the_index_lookup_withholds_of_the_asked_index_fails_it(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    # I:SPX's request is answered with a bare SPX row the lookup can't classify: the row could
    # be I:SPX's, and it was withheld, so I:SPX, with no row, fails for it.
    fake_rest.reply(BY_UNDERLYING_PATH, Reply(envelope([_underlying_open_interest("SPX", 3)])))
    _serve_index_list(fake_rest, "SPX")
    fake_rest.reply(CONTRACTS_LIST_PATH, Reply(envelope([])))
    out = _fetch(session, options.OPEN_INTEREST_BY_UNDERLYING, ["I:SPX"])
    assert str(_failure(out["I:SPX"]).error).startswith(
        "options.open_interest_by_underlying: the library could not read the 1 row Massive "
        "sent for I:SPX from GET /options/v1/open-interest/underlying whole, so it withheld "
        "it. Massive's contracts list has no contract with a CFI code for SPX"
    )


def test_a_request_result_no_case_handles_is_refused(
    *, session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    def answered(self: object, spec: object, units: Sequence[object], **_: object) -> object:
        return SimpleNamespace(units=units, results=["bogus"] * len(units), requests=0)

    monkeypatch.setattr(rest.RestReader, "read_each", answered)
    with pytest.raises(AssertionError, match="got: 'bogus'"):
        _fetch(session, DIVIDENDS, ["AAPL"])


@pytest.mark.parametrize("setting", list(dm.OnError))
def test_a_request_that_lists_one_symbol_keeps_only_its_rows(
    *,
    session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
    setting: dm.OnError,
) -> None:
    # A list of one symbol follows the rule of a list of several: a row of another symbol is
    # left out and is the read's problem. Under RAISE the request's symbol fails instead.
    events = _problems(session, monkeypatch)
    rows = [_dividend("AAPL", 0.25), _dividend("IBM", 1.67), _dividend("IBM", 1.68)]
    fake_rest.reply(DIVIDENDS_PATH, Reply(envelope(rows)))
    with caplog.at_level(logging.DEBUG):
        out = _fetch(session, DIVIDENDS, ["AAPL"], unrequested=setting)
    assert [query["ticker.any_of"] for _, query in fake_rest.get_queries()] == [["AAPL"]]
    assert _zeroed(events) == [_unrequested("IBM", 2, asked="AAPL", setting=setting)]
    assert len(alarms(caplog)) == (1 if setting is dm.OnError.LOG else 0)
    if setting is dm.OnError.RAISE:
        failed = _failure(out["AAPL"])
        assert isinstance(failed.error, RowDataError)
        assert not failed.error.is_retryable
        assert str(failed.error) == (
            "stocks.dividends: Massive answered the request for AAPL with 2 rows of IBM, "
            "which the read did not ask for, so the library left them out. To go on without "
            "such rows, pass on_error=dm.ErrorPolicy(unrequested_symbols=dm.OnError.LOG) to the "
            "call or to dm.MassiveConfig."
        )
    else:
        assert [(r["Symbol"], r["CashAmount"]) for r in _rows(out["AAPL"])] == [("AAPL", 0.25)]


@pytest.mark.parametrize("setting", [dm.OnError.LOG, dm.OnError.RAISE])
def test_a_row_left_out_is_named_as_massive_sent_it(
    *,
    session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    monkeypatch: pytest.MonkeyPatch,
    setting: dm.OnError,
) -> None:
    # Massive writes the index bare, `NDX`, and the table would name it `I:NDX`: the problem
    # names the row as Massive sent it, and its detail and the failure give the table's
    # spelling. The symbol's own table has the dataset's columns and nothing else.
    events = _problems(session, monkeypatch)
    page = [{"ticker": "I:SPX", "value": 1.0}, {"ticker": "NDX", "value": 2.0}]
    fake_rest.reply(INDEX_PATH, Reply(envelope(page)))
    out = _fetch(session, INDEX, ["I:SPX"], unrequested=setting)
    assert _zeroed(events) == [
        _unrequested(
            "NDX",
            1,
            asked="I:SPX",
            setting=setting,
            spec=INDEX,
            where=f"REST GET {INDEX_PATH}",
            detail=(
                "Massive answered the request for I:SPX with rows of NDX, which the read did "
                "not ask for, so they are left out. The table spells NDX as I:NDX."
            ),
        )
    ]
    if setting is dm.OnError.RAISE:
        failed = _failure(out["I:SPX"])
        assert isinstance(failed.error, RowDataError)
        assert str(failed.error) == (
            "indices.snapshots: Massive answered the request for I:SPX with 1 row of NDX, "
            "which the read did not ask for, so the library left it out. The table spells NDX "
            "as I:NDX. To go on without such rows, pass on_error=dm.ErrorPolicy("
            "unrequested_symbols=dm.OnError.LOG) to the call or to dm.MassiveConfig."
        )
    else:
        rows = _rows(out["I:SPX"])
        assert [(r["Symbol"], r["Value"]) for r in rows] == [("I:SPX", 1.0)]
        assert list(rows[0]) == session._core.dataset_schema(INDEX).arrow_schema().names


@pytest.mark.parametrize(
    ("spec", "template", "expected"),
    [
        (RELATED, RELATED_PATH, "Underlying"),
        (DIVIDENDS, DIVIDENDS_PATH, "Symbol"),
        (TRADES, TRADES_PATH, "Symbol"),
        (PREV, PREV_PATH, "Symbol"),
    ],
)
def test_the_asked_column_is_the_fan_out_key_filled_from_the_request_else_symbol(
    spec: DatasetSpec, template: str, expected: str
) -> None:
    assert asked_column(spec, template) == expected


def test_a_path_that_fills_its_fan_out_key_from_the_request_is_attributed_by_it(
    *,
    session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The related companies of AAPL name the related company in Symbol; the asked symbol is
    # Underlying, filled from the request, so every row is AAPL's and none is unrequested.
    events = _problems(session, monkeypatch)
    related = [{"ticker": "MSFT"}, {"ticker": "GOOG"}]
    fake_rest.reply("/v1/related-companies/AAPL", Reply(envelope(related)))
    with caplog.at_level(logging.DEBUG):
        out = _fetch(session, RELATED, ["AAPL"], unrequested=dm.OnError.RAISE)
    assert [(r["Underlying"], r["Symbol"]) for r in _rows(out["AAPL"])] == [
        ("AAPL", "MSFT"),
        ("AAPL", "GOOG"),
    ]
    assert events == []
    assert alarms(caplog) == []


@pytest.mark.parametrize("setting", list(dm.OnError))
def test_a_request_by_a_symbol_s_own_path_keeps_only_the_rows_that_name_it(
    *,
    session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
    setting: dm.OnError,
) -> None:
    # A request for one symbol by its own path follows the rule of a request that lists
    # several: a row whose echoed ticker names another symbol is left out and is the read's
    # problem, and under RAISE the request's symbol fails.
    events = _problems(session, monkeypatch)
    rows = [{"T": "AAPL", "c": 1.0}, {"T": "MSFT", "c": 2.0}]
    fake_rest.reply("/v2/aggs/ticker/AAPL/prev", Reply(envelope(rows)))
    with caplog.at_level(logging.DEBUG):
        out = _fetch(session, PREV, ["AAPL"], unrequested=setting)
    assert [path for path, _ in fake_rest.get_queries()] == ["/v2/aggs/ticker/AAPL/prev"]
    assert _zeroed(events) == [
        _unrequested(
            "MSFT",
            1,
            asked="AAPL",
            setting=setting,
            spec=PREV,
            where="REST GET /v2/aggs/ticker/{stocksTicker}/prev",
        )
    ]
    assert len(alarms(caplog)) == (1 if setting is dm.OnError.LOG else 0)
    if setting is dm.OnError.RAISE:
        failed = _failure(out["AAPL"])
        assert isinstance(failed.error, RowDataError)
        assert str(failed.error) == (
            "stocks.previous_day_bar: Massive answered the request for AAPL with 1 row of "
            "MSFT, which the read did not ask for, so the library left it out. To go on without "
            "such rows, pass on_error=dm.ErrorPolicy(unrequested_symbols=dm.OnError.LOG) to the "
            "call or to dm.MassiveConfig."
        )
    else:
        assert [(r["Symbol"], r["Close"]) for r in _rows(out["AAPL"])] == [("AAPL", 1.0)]


def test_a_listed_request_s_row_naming_no_symbol_belongs_to_none(
    *,
    session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A dataset with an optional ticker keeps a null Symbol on a row without one; such a row of
    # a list request names no symbol of the list, at any length of the list.
    events = _problems(session, monkeypatch)
    rows = [{"ticker": "AAPL", "rating": "Buy"}, {"rating": "Hold"}]
    fake_rest.reply(RATINGS_PATH, Reply(envelope(rows)))
    with caplog.at_level(logging.DEBUG):
        out = _fetch(session, RATINGS, ["AAPL"])
    assert [r["Symbol"] for r in _rows(out["AAPL"])] == ["AAPL"]
    assert [(e.symbol, e.rows, e.detail) for e in events] == [
        (
            None,
            1,
            (
                "Massive answered the request for AAPL with rows of no symbol, which the "
                "read did not ask for, so they are left out"
            ),
        )
    ]
    assert alarms(caplog) == [
        (
            "partners.benzinga.ratings table 7: unrequested_symbols: 1 row, Massive answered the "
            "request for AAPL with rows of no symbol, which the read did not ask for, so "
            "they are left out (at REST GET /benzinga/v1/ratings). m.problems() lists it and "
            "every later unrequested_symbols problem of this table, which the log shows at "
            "DEBUG only."
        )
    ]


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


def _unlisted(
    rows: int, *, asked: str, only: str, setting: dm.OnError = dm.OnError.LOG
) -> ProblemEvent:
    # The event of a ticker-list request's rows that list no ticker it asked for.
    return _unrequested(
        None,
        rows,
        asked=asked,
        setting=setting,
        spec=LISTED,
        where="REST GET /benzinga/v2/news",
        field="Tickers",
        detail=f"Massive answered the request for {asked} with rows whose Tickers lists no "
        f"ticker asked for{only} so they are left out",
    )


def test_a_row_listing_none_of_the_named_tickers_belongs_to_no_symbol(
    *,
    session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The row is about none of the request's symbols, so it is left out and is the read's
    # problem, alarmed once; a row listing nothing counts too.
    events = _problems(session, monkeypatch)
    fake_rest.reply(
        BENZINGA_NEWS_PATH,
        Reply(
            envelope(
                [
                    {"benzinga_id": 1, "tickers": ["MSFT"]},
                    {"benzinga_id": 2, "tickers": ["IBM", "TSLA"]},
                    {"benzinga_id": 3, "tickers": []},
                ]
            )
        ),
    )
    with caplog.at_level(logging.DEBUG):
        out = _fetch(session, LISTED, ["MSFT", "AAPL"])
    assert _rows(out["AAPL"]) == []
    assert [(r["BenzingaId"], r["Symbol"]) for r in _rows(out["MSFT"])] == [(1, "MSFT")]
    assert _zeroed(events) == [_unlisted(2, asked="AAPL and MSFT", only=", only IBM and TSLA,")]
    assert alarms(caplog) == [
        (
            "partners.benzinga.news table 7: unrequested_symbols: 2 rows, Massive answered the "
            "request for AAPL and MSFT with rows whose Tickers lists no ticker asked for, only "
            "IBM and TSLA, so they are left out (at REST GET /benzinga/v2/news). m.problems() "
            "lists it and every later unrequested_symbols problem of this table, which the log "
            "shows at DEBUG only."
        )
    ]


@pytest.mark.parametrize("setting", list(dm.OnError))
def test_a_request_that_names_one_ticker_keeps_only_the_rows_that_list_it(
    *,
    session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
    setting: dm.OnError,
) -> None:
    # One named ticker follows the rule of several: a row listing none of them is left out and
    # is the read's problem, and under RAISE the request's ticker fails.
    events = _problems(session, monkeypatch)
    articles = [{"benzinga_id": 1, "tickers": ["TSLA"]}, {"benzinga_id": 2, "tickers": ["AAPL"]}]
    fake_rest.reply(BENZINGA_NEWS_PATH, Reply(envelope(articles)))
    with caplog.at_level(logging.DEBUG):
        out = _fetch(session, LISTED, ["AAPL"], unrequested=setting)
    assert [query["tickers.any_of"] for _, query in fake_rest.get_queries()] == [["AAPL"]]
    assert _zeroed(events) == [_unlisted(1, asked="AAPL", only=", only TSLA,", setting=setting)]
    assert len(alarms(caplog)) == (1 if setting is dm.OnError.LOG else 0)
    if setting is dm.OnError.RAISE:
        failed = _failure(out["AAPL"])
        assert isinstance(failed.error, RowDataError)
        assert not failed.error.is_retryable
        assert str(failed.error) == (
            "partners.benzinga.news: Massive answered the request for AAPL with 1 row whose "
            "Tickers lists no ticker asked for, only TSLA, so the library left it out. To go on "
            "without such rows, pass on_error=dm.ErrorPolicy(unrequested_symbols=dm.OnError.LOG) "
            "to the call or to dm.MassiveConfig."
        )
    else:
        assert [(r["BenzingaId"], r["Symbol"]) for r in _rows(out["AAPL"])] == [(2, "AAPL")]


@pytest.mark.parametrize("setting", list(dm.OnError))
def test_a_crypto_pair_massive_does_not_list_has_no_rows_or_fails_alone_under_raise(
    *,
    session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    monkeypatch: pytest.MonkeyPatch,
    setting: dm.OnError,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # A pair Massive has no data for, as a 404 is: no rows under LOG and IGNORE, logged once at
    # DEBUG and warned of under LOG only; its own failure under RAISE.
    core = session._core
    replaced = dataclasses.replace(
        core.config, on_error=dm.ErrorPolicy(unavailable_symbols=setting)
    )
    monkeypatch.setattr(core, "config", replaced)
    listed = {"ticker": "X:BTCUSD", "base_currency_symbol": "BTC", "currency_symbol": "USD"}
    fake_rest.reply(TICKERS_PATH, Reply(envelope([listed])), Reply(envelope([])))
    last = {
        "status": "OK",
        "symbol": "BTC-USD",
        "last": {"price": 90000.5, "timestamp": 1741000000000},
    }
    fake_rest.reply("/v1/last/crypto/BTC/USD", Reply(last))
    with caplog.at_level(logging.DEBUG, logger=REST_LOGGER):
        out = _fetch(session, CRYPTO_LAST, ["X:NOPEUSD", "X:BTCUSD"])
    assert [r["Symbol"] for r in _rows(out["X:BTCUSD"])] == ["X:BTCUSD"]
    skipped = [
        (r.levelno, r.getMessage()) for r in caplog.records if "are left out" in r.getMessage()
    ]
    warned = alarms(caplog)
    assert len(warned) == (1 if setting is dm.OnError.LOG else 0)
    assert all(
        "unavailable_symbols: X:NOPEUSD: Massive has no data for it (Massive's crypto ticker "
        "list does not hold it)" in message
        for message in warned
    )
    if setting is dm.OnError.RAISE:
        assert skipped == []
    else:
        assert _rows(out["X:NOPEUSD"]) == []
        assert skipped == [
            (
                logging.DEBUG,
                (
                    "crypto.last: Massive has no data for 1 [X:NOPEUSD (Massive's crypto "
                    "ticker list does not hold it)], so they are left out"
                ),
            )
        ]
    if setting is dm.OnError.RAISE:
        nope = _failure(out["X:NOPEUSD"])
        assert isinstance(nope.error, UnavailableSymbolError)
        assert str(nope.error) == (
            "crypto.last: Massive has no data for X:NOPEUSD. Remove it, or, to read the others, "
            "pass on_error=dm.ErrorPolicy(unavailable_symbols=dm.OnError.LOG) to the call or to "
            "dm.MassiveConfig."
        )
        assert not nope.error.is_retryable
    # No request was made for the pair the list does not hold.
    assert [p for p, _ in fake_rest.get_queries() if p != TICKERS_PATH] == [
        "/v1/last/crypto/BTC/USD"
    ]


def test_a_parameter_the_path_does_not_take_raises_before_any_request(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    # The call refuses such a parameter for every path its symbol table could reach, so a
    # fetch read by symbol never fails one symbol for it.
    with pytest.raises(ValueError, match=r"^stocks\.dividends does not take adjusted=False\.$"):
        _fetch(session, DIVIDENDS, ["AAPL", "MSFT"], adjusted=False)
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
    # The REST read raises at the first request the closed session refuses.
    rest = session._core.rest_transport()
    session.close()
    with pytest.raises(dm.MassiveError, match="^" + re.escape(session_closed()) + "$"):
        rest.fetch_by_symbol(DIVIDENDS, Request(), symbols=["AAPL"], call=7)


def test_a_session_closed_before_the_outcomes_are_built_fails_the_read(
    session: dm.MassiveSession,
) -> None:
    # A flat-file scan builds its outcomes after reading: a session closed meanwhile fails the
    # read the same way.
    session.close()
    with pytest.raises(dm.SessionClosedError, match="^" + re.escape(session_closed()) + "$"):
        symbol_outcomes(
            session._core, DIVIDENDS, symbols=["AAPL"], tables={}, failures={}, call=7, source="x"
        )


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
    retryable = {
        symbol: _failure(out[symbol]).error.is_retryable
        for symbol in ["NVDA", "TSLA", "AMZN", "WMT"]
    }
    assert retryable == {
        "NVDA": False,
        "TSLA": True,
        "AMZN": True,
        "WMT": True,
    }
    assert isinstance(_failure(out["AMZN"]).error, CutShortError)
    assert str(_failure(out["AMZN"]).error).startswith(
        "stocks.trades: the rows of AMZN were cut short after 1 page arrived. "
    )
    assert str(_failure(out["WMT"]).error).startswith("Massive answered GET ")
    # MSFT's missing data warns as history() warns it, as a problem; no failure warns.
    warned = [r for r in caplog.records if r.name in {LOGGER, REST_LOGGER}]
    assert [r.getMessage() for r in warned if r.levelno >= logging.WARNING] == []
    (alarm,) = alarms(caplog)
    assert alarm == (
        "stocks.trades table 7: unavailable_symbols: MSFT: Massive has no data for it (HTTP 404 "
        "from GET /v3/trades/MSFT) (at stocks.trades). m.problems() lists it and every later "
        "unavailable_symbols problem of this table, which the log shows at DEBUG only."
    )


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
    out = rest.history_by_symbol(TRADES, asked, window, symbols=["AAPL"], call=7, tally=ReadTally())
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
    out = rest.history_by_symbol(
        TRADES, asked, window, symbols=["AAPL", "MSFT"], call=7, tally=ReadTally()
    )
    assert [_rows(outcome) for outcome in out.values()] == [[], []]
    assert fake_rest.get_queries() == []


def test_a_history_parameter_the_path_does_not_take_fails_every_symbol(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    out = _history(session, ["AAPL", "MSFT"], _monday(), adjusted=False)
    assert not any(_failure(o).error.is_retryable for o in out.values())
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


def _split(
    m: dm.MassiveSession, table: Table, symbols: list[str], *, setting: dm.OnError
) -> dict[str, Table]:
    return split_by_symbol(
        m._core,
        FLAT_TRADES,
        table,
        symbols=symbols,
        source="flat files",
        policy=setting,
        call=7,
    )


@pytest.mark.parametrize("setting", list(dm.OnError))
def test_a_split_by_symbol_reports_rows_no_symbol_owns(
    *,
    massive_session: dm.MassiveSession,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
    setting: dm.OnError,
) -> None:
    # A row of another symbol, or of none, is left out as the read's problem, one event per
    # symbol such rows name; under RAISE the read fails, which is every symbol's failure.
    events = _problems(massive_session, monkeypatch)
    table = _flat_rows(massive_session, ["AAPL", "MSFT", "AAPL", "IBM", None])
    asked = ["NVDA", "AAPL", "MSFT"]
    expected = [
        _unrequested(
            symbol,
            1,
            asked="NVDA, AAPL, and MSFT",
            setting=setting,
            spec=FLAT_TRADES,
            where="flat files",
            detail=f"The read of flat files for NVDA, AAPL, and MSFT returned rows of "
            f"{'no symbol' if symbol is None else symbol}, which the read did not ask for, so "
            "they are left out",
        )
        for symbol in ("IBM", None)
    ]
    with caplog.at_level(logging.DEBUG):
        if setting is dm.OnError.RAISE:
            with pytest.raises(RowDataError) as raised:
                _split(massive_session, table, asked, setting=setting)
            assert str(raised.value) == (
                "stocks.trades: the read of flat files for NVDA, AAPL, and MSFT returned 2 rows "
                "of IBM and no symbol, which the read did not ask for, so the library left "
                "them out. To go on without such rows, pass "
                "on_error=dm.ErrorPolicy(unrequested_symbols=dm.OnError.LOG) to the call or to "
                "dm.MassiveConfig."
            )
        else:
            out = _split(massive_session, table, asked, setting=setting)
            assert list(out) == asked
            assert [part.size for part in out.values()] == [0, 2, 1]
    assert _zeroed(events) == expected
    assert len(alarms(caplog)) == (1 if setting is dm.OnError.LOG else 0)


def test_a_split_that_keeps_every_row_reports_nothing(
    *,
    massive_session: dm.MassiveSession,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events = _problems(massive_session, monkeypatch)
    table = _flat_rows(massive_session, ["AAPL"])
    with caplog.at_level(logging.DEBUG):
        _split(massive_session, table, ["AAPL"], setting=dm.OnError.RAISE)
    assert events == []
    assert alarms(caplog) == []


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
    assert not failure.error.is_retryable
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


UNREADABLE_NQ = Reply(envelope([{"product_code": "NQ", "last_updated": "no time"}]))
"""NQ's product, whose only row has an update time the library cannot read."""


def test_a_value_whose_rows_are_all_unreadable_fails_alone(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    # Each value is judged by its own rows: NQ's one row is withheld, so NQ fails, naming
    # itself, and ES keeps its rows.
    es = Reply(envelope([{"product_code": "ES", "name": "E-mini S&P 500"}]))
    fake_rest.reply(PRODUCTS_PATH, es, UNREADABLE_NQ)
    out = _by_value(rest_session, ["ES", "NQ"])
    assert [r["ProductCode"] for r in _rows(out["ES"])] == ["ES"]
    error = _failure(out["NQ"]).error
    assert isinstance(error, RowDataError)
    assert str(error).startswith(
        "futures.products: the library could not read the 1 row Massive sent for NQ from "
        "GET /futures/v1/products whole, so it withheld it. "
    )


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
    assert str(error) == (
        "futures.products does not take adjusted=False. Leave that argument out, or read these "
        "symbols in a call without it."
    )
    assert fake_rest.get_queries() == []
