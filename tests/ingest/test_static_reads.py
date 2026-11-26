"""Tests for a static `fetch()` or REST `history()` of symbols in code: each failure its own.

The reads run against the fake REST server. Each test records the spans the read lists in
`m.problems()`, so it can check that every asked symbol without rows is listed (no silent gap).
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any

import pytest
from deephaven import arrow as dh_arrow

import deephaven_massive as dm
from deephaven_massive import _session
from deephaven_massive._datasets.catalog import Catalog, DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._datasets.futures_sessions import CHICAGO
from deephaven_massive._datasets.row_times import RowTimes
from deephaven_massive._enums import AssetClass, HistorySource
from deephaven_massive._ingest.symbol_reads import asked_column
from deephaven_massive._massive import Transport
from deephaven_massive._tables.problem_events import ProblemCause, ProblemEvent, ProblemSpans
from deephaven_massive.namespaces._query_building import build_query
from deephaven_massive.queries import HistoryQuery, Query
from tests._fakes.rest import FakeRestServer, Reply, envelope

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence

    from deephaven.table import Table

    from deephaven_massive._tables.problem_events import Problem

PREV = "/v2/aggs/ticker/{}/prev"
FUTURES_TRADES_PATH = "/futures/v1/trades/{ticker}"
FUTURES_TRADES = DatasetSpec(
    summary="A dataset for the tests.",
    name="futures.trades",
    asset_class=AssetClass.FUTURES,
    row_times=RowTimes.TICK,
    history_path=FUTURES_TRADES_PATH,
    time_axis=("timestamp.gte", "timestamp.lt"),
    keys={
        (Transport.REST, FUTURES_TRADES_PATH): SourceKeys(
            KeySpec("ticker", from_request=True),
            KeySpec("Timestamp"),
            date=KeySpec("SessionEndDate"),
        )
    },
)


class Recorded:
    """The problems a session's reads report, each still passed on to its reporter."""

    def __init__(self, m: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch) -> None:
        self.found: list[Problem] = []
        report = m._core.problems

        def recorded(found: Sequence[Problem]) -> None:
            self.found.extend(found)
            report(found)

        monkeypatch.setattr(m._core, "problems", recorded)

    def spans(self) -> list[tuple[ProblemCause, str]]:
        # Each span that held, by cause and name, in the order reported.
        return [
            (problem.cause, name)
            for problem in self.found
            if isinstance(problem, ProblemSpans)
            for name in problem.spans
        ]

    def events(self) -> list[tuple[ProblemCause, str | None]]:
        # Each event, by cause and symbol, in the order reported.
        return [
            (problem.cause, problem.symbol)
            for problem in self.found
            if isinstance(problem, ProblemEvent)
        ]


def prev_bar(ticker: str) -> Reply:
    bar = {"T": ticker, "o": 1.0, "h": 2.0, "l": 0.5, "c": 1.5, "v": 100, "t": 1605042000000}
    return Reply({"status": "OK", "ticker": ticker, "results": [bar], "resultsCount": 1})


REFUSED = Reply({"status": "NOT_AUTHORIZED", "message": "Not entitled."}, status=403)


def symbols_of(table: Table) -> list[str]:
    return [str(s) for s in dh_arrow.to_arrow(table).column("Symbol").to_pylist()]


def test_one_refused_symbol_leaves_the_others_rows_and_is_listed(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake_rest.reply(PREV.format("AAPL"), REFUSED)
    fake_rest.reply(PREV.format("MSFT"), prev_bar("MSFT"))
    fake_rest.reply(PREV.format("NVDA"), prev_bar("NVDA"))
    recorded = Recorded(rest_session, monkeypatch)
    asked = ["AAPL", "MSFT", "NVDA"]
    out = symbols_of(rest_session.stocks.previous_day_bar(asked).fetch(wait=True))
    assert out == ["MSFT", "NVDA"]
    assert recorded.spans() == [(ProblemCause.REFUSED_SYMBOLS, "AAPL")]
    # No silent gap: each asked symbol without rows is listed.
    assert set(asked) - set(out) == {name for _, name in recorded.spans()}


def test_a_call_whose_every_symbol_is_refused_raises_and_lists_each(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake_rest.reply(PREV.format("AAPL"), REFUSED)
    fake_rest.reply(PREV.format("MSFT"), REFUSED)
    recorded = Recorded(rest_session, monkeypatch)
    with pytest.raises(dm.EntitlementError, match=r"GET /v2/aggs/ticker/AAPL/prev"):
        rest_session.stocks.previous_day_bar(["AAPL", "MSFT"]).fetch(wait=True)
    # The error gives the first symbol's reason; m.problems() lists each with its own.
    assert recorded.spans() == [
        (ProblemCause.REFUSED_SYMBOLS, "AAPL"),
        (ProblemCause.REFUSED_SYMBOLS, "MSFT"),
    ]


def test_refused_symbols_set_to_raise_fails_the_call_once_the_symbol_is_listed(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake_rest.reply(PREV.format("AAPL"), REFUSED)
    fake_rest.reply(PREV.format("MSFT"), prev_bar("MSFT"))
    recorded = Recorded(rest_session, monkeypatch)
    strict = dm.ErrorPolicy(refused_symbols=dm.OnError.RAISE)
    with pytest.raises(dm.EntitlementError, match="Not entitled"):
        rest_session.stocks.previous_day_bar(["AAPL", "MSFT"], on_error=strict).fetch(wait=True)
    assert recorded.spans() == [(ProblemCause.REFUSED_SYMBOLS, "AAPL")]


def test_a_symbol_without_data_under_raise_fails_the_call_once_it_is_listed(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake_rest.reply(PREV.format("MSFT"), prev_bar("MSFT"))  # AAPL answers HTTP 404
    recorded = Recorded(rest_session, monkeypatch)
    strict = dm.ErrorPolicy(unavailable_symbols=dm.OnError.RAISE)
    with pytest.raises(dm.MassiveError, match="Massive has no data for AAPL"):
        rest_session.stocks.previous_day_bar(["AAPL", "MSFT"], on_error=strict).fetch(wait=True)
    assert recorded.events() == [(ProblemCause.UNAVAILABLE_SYMBOLS, "AAPL")]
    assert recorded.spans() == []


@pytest.fixture
def futures_session(
    fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch
) -> Iterator[dm.MassiveSession]:
    monkeypatch.setattr(_session, "CATALOG", Catalog((FUTURES_TRADES,)))
    m = dm.MassiveSession(dm.MassiveConfig(api_key="test-key", rest_base_url=fake_rest.url))
    yield m
    m.close()


def futures_trade(ticker: str, day: int) -> dict[str, Any]:
    moment = datetime(2025, 1, day, 10, tzinfo=CHICAGO)
    return {
        "ticker": ticker,
        "timestamp": int(moment.timestamp()) * 10**9,
        "sequence_number": 1,
        "report_sequence": 1,
        "channel": 1,
        "price": 1.0,
        "session_end_date": f"2025-01-{day}",
    }


def session_target(ticker: str, day: int) -> str:
    return (
        f"/futures/v1/trades/{ticker}?limit=50000&sort=timestamp.asc&session_end_date=2025-01-{day}"
    )


def test_a_failed_symbol_s_rows_from_its_other_requests_are_left_out(
    *,
    futures_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Each session is its own request: ESH5's second session fails, so its first session's
    # rows are left out too, and a table never holds part of a failed symbol's data.
    for day in (21, 22):
        fake_rest.reply(session_target("NQH5", day), Reply(envelope([futures_trade("NQH5", day)])))
    fake_rest.reply(session_target("ESH5", 21), Reply(envelope([futures_trade("ESH5", 21)])))
    fake_rest.reply(session_target("ESH5", 22), Reply(b"oops", status=500))
    recorded = Recorded(futures_session, monkeypatch)
    q = build_query(
        futures_session._core, FUTURES_TRADES, query_type=Query, symbols=["ESH5", "NQH5"]
    )
    assert isinstance(q, HistoryQuery)
    out = q.history("2025-01-21", "2025-01-22", source=HistorySource.REST, wait=True)
    assert symbols_of(out) == ["NQH5", "NQH5"]
    assert recorded.spans() == [(ProblemCause.SYMBOL_READ_FAILED, "ESH5")]


def test_every_dataset_read_by_symbol_has_the_column_its_rows_are_given_by() -> None:
    # A static read gives each row to the asked symbol its column names (`asked_column`).
    catalog = _session.CATALOG
    missing = [
        (spec.name, template, asked_column(spec, template))
        for spec in catalog.by_name.values()
        if catalog.takes_symbols(spec)
        for template in spec.rest_paths
        if asked_column(spec, template) not in catalog.dataset_schema(spec).arrow_schema().names
    ]
    assert missing == []
