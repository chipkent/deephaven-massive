"""Tests for `queries/_fetch.py`: `fetch()` and `poll()`, of symbols and of tables of values."""

import dataclasses
import logging
import re
import threading
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from deephaven import arrow as dh_arrow
from deephaven import dtypes, new_table
from deephaven.column import string_col
from deephaven.table import Table
from deephaven.table_factory import InputTable

import deephaven_massive as dm
from deephaven_massive._datasets.catalog import DatasetSpec
from deephaven_massive._enums import (
    HistorySource,
    OnError,
    PollMode,
)
from deephaven_massive._errors import UnlistedPairError
from deephaven_massive._ingest import rest as rest_module
from deephaven_massive._massive import Patience
from deephaven_massive._tables.execution_context import entered
from deephaven_massive._tables.transports import (
    FailureKind,
    SymbolFailure,
    SymbolRows,
)
from deephaven_massive.namespaces import _futures as futures
from deephaven_massive.namespaces._options import CHAIN
from deephaven_massive.queries import (
    FetchQuery,
)
from deephaven_massive.queries._fetch import MINIMUM_POLL_INTERVAL, _symbols_per_read
from tests._fakes.queries import (
    LAST_TRADE_SPEC,
    Recorder,
    feed,
    follows,
    join_reads,
    last_trade,
    make,
    news,
    released,
    symbol_reads,
    trades,
    until,
)
from tests._fakes.rest import FakeRestServer, Received, Reply, envelope


def test_fetch_and_poll_delegate(
    massive_session: dm.MassiveSession, rest: Recorder, caplog: pytest.LogCaptureFixture
) -> None:
    q = feed(massive_session)
    q.fetch(wait=True)
    with caplog.at_level(logging.DEBUG, logger="deephaven_massive.queries"):
        q.poll(30)
        q.poll(30, mode=PollMode.AUTO)
        q.poll(timedelta(minutes=1), mode=PollMode.REPLACE)
    shown = repr(q)
    # Each poll() logs the mode it asked for and the mode it got.
    assert [m for m in caplog.messages if " poll(): " in m] == [
        f"{shown} poll(): every 30.000 s, mode AUTO -> APPEND",
        f"{shown} poll(): every 30.000 s, mode AUTO -> APPEND",
        f"{shown} poll(): every 60.000 s, mode REPLACE -> REPLACE",
    ]
    # AUTO, the default, is the dataset's own mode: APPEND for this feed.
    assert [(c[0], c[3]) for c in rest.calls] == [
        ("fetch", {}),
        ("poll", {"interval": timedelta(seconds=30), "mode": PollMode.APPEND, "query": shown}),
        ("poll", {"interval": timedelta(seconds=30), "mode": PollMode.APPEND, "query": shown}),
        ("poll", {"interval": timedelta(minutes=1), "mode": PollMode.REPLACE, "query": shown}),
    ]


@pytest.mark.parametrize("mode", [PollMode.AUTO, PollMode.REPLACE])
def test_a_feed_without_start_refuses_to_poll(
    *,
    massive_session: dm.MassiveSession,
    rest: Recorder,
    mode: PollMode,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Every cycle would re-read Massive's whole archive; the message names the fix.
    message = (
        "m.economy.inflation().poll() needs a start or a limit: without either, every cycle "
        "re-reads the whole archive Massive keeps. Build the query with a start, such as "
        "m.economy.inflation(..., start=dh_today(to_j_time_zone('America/New_York'))), or "
        "with a limit and a newest-first order, then call poll(). A fixed start keeps widening "
        "the window on a poll that runs for days"
    )
    with (
        pytest.raises(ValueError, match=f"^{re.escape(message)}$"),
        caplog.at_level(logging.DEBUG, logger="deephaven_massive"),
    ):
        news(massive_session).poll(30, mode=mode)
    assert rest.calls == []
    # The error is the user's report; the log has the decision and the refusal, at DEBUG.
    assert [r.levelno for r in caplog.records] == [logging.DEBUG, logging.DEBUG]
    assert caplog.messages[1] == f"m.economy.inflation().poll() raised ValueError: {message}"
    news(massive_session).fetch(wait=True)  # fetch() reads the whole range, as asked
    assert [c[0] for c in rest.calls] == ["fetch"]


def test_a_feed_with_a_limit_polls_its_latest_rows(
    massive_session: dm.MassiveSession, rest: Recorder
) -> None:
    # The "latest headlines" poll: each cycle reads at most `limit` rows, never the archive.
    news(massive_session, limit=50).poll(60)
    assert [c[0] for c in rest.calls] == ["poll"]


@pytest.mark.parametrize("interval", [0, -1, float("inf"), float("nan"), 1e300, 10**400])
def test_poll_interval_values(
    massive_session: dm.MassiveSession, rest: Recorder, interval: object
) -> None:
    with pytest.raises(ValueError, match=r"interval must be (positive|a positive, finite)"):
        feed(massive_session).poll(interval)  # type: ignore[arg-type]  # the point: refused at run time
    assert rest.calls == []


@pytest.mark.parametrize(
    "interval",
    [0.999, timedelta(milliseconds=999), 1e-9, timedelta(0), timedelta(seconds=-5)],
)
def test_poll_refuses_an_interval_under_one_second(
    massive_session: dm.MassiveSession, rest: Recorder, interval: float | timedelta
) -> None:
    # A typo such as poll(0.01) for poll(10) would send requests back to back on a plan
    # Massive does not pace; the error names the minimum and the value given.
    message = (
        f"interval must be at least 1 second, got {interval!r}: each poll() cycle sends a "
        "request; use a stream for faster updates"
    )
    with pytest.raises(ValueError, match=f"^{re.escape(message)}$"):
        feed(massive_session).poll(interval)
    assert rest.calls == []


@pytest.mark.parametrize("interval", [1, 1.0, timedelta(seconds=1)])
def test_poll_accepts_an_interval_of_one_second(
    massive_session: dm.MassiveSession, rest: Recorder, interval: float | timedelta
) -> None:
    feed(massive_session).poll(interval)
    assert [(c[0], c[3]["interval"]) for c in rest.calls] == [("poll", MINIMUM_POLL_INTERVAL)]


@pytest.mark.parametrize("interval", ["30", True, None])
def test_poll_interval_types(
    massive_session: dm.MassiveSession, rest: Recorder, interval: object
) -> None:
    with pytest.raises(TypeError, match="interval must be a number"):
        feed(massive_session).poll(interval)  # type: ignore[arg-type]  # the point: refused at run time


def test_poll_mode_is_an_enum(massive_session: dm.MassiveSession, rest: Recorder) -> None:
    with pytest.raises(
        TypeError, match=r"^mode must be dm\.PollMode, e\.g\. dm\.PollMode\.UPSERT$"
    ):
        feed(massive_session).poll(
            30,
            mode="upsert",  # type: ignore[arg-type]  # the point: refused at run time
        )


def test_a_one_row_fetch_passes_its_symbols(
    massive_session: dm.MassiveSession, rest: Recorder
) -> None:
    last_trade(massive_session).fetch(wait=True)
    assert [(c[0], c[1], c[2].symbols) for c in rest.calls] == [
        ("fetch", LAST_TRADE_SPEC, ("AAPL",))
    ]


def test_only_symbols_in_code_have_their_crypto_pairs_split_at_the_call(
    massive_session: dm.MassiveSession, rest: Recorder
) -> None:
    # A pair in code is code: one Massive does not list raises at the call. A table's pairs
    # are data, which the read takes as MassiveConfig.unlisted_symbols says.
    in_code = news(massive_session).fetch()
    table = new_table([string_col("Symbol", ["AAPL"])])
    from_table = trades(massive_session, table).history("2025-03-03", source=HistorySource.REST)
    listed = trades(massive_session).history("2025-03-03", source=HistorySource.REST)
    join_reads(massive_session)
    assert rest.split_pairs == [True, False, True]
    released(massive_session, in_code, from_table, listed)


def rows_of(m: dm.MassiveSession, spec: DatasetSpec, count: int) -> SymbolRows:
    # A symbol's static rows, as a read returns them: `count` rows of nulls.
    schema = m._core.dataset_schema(spec)
    with entered(m._core.exec_ctx):
        return SymbolRows(dh_arrow.to_table(schema.to_arrow([{}] * count)))


def test_a_static_fetch_is_unchanged_and_passes_the_unlisted_setting(
    *, massive_session: dm.MassiveSession, rest: Recorder
) -> None:
    static = new_table([string_col("Symbol", ["AAPL"])])
    q = make(massive_session, LAST_TRADE_SPEC, symbols=static)
    assert isinstance(q, FetchQuery)
    assert not q.fetch(wait=True).is_refreshing
    assert not last_trade(massive_session).fetch(wait=True).is_refreshing
    # A symbol table's crypto pairs follow the session's setting; symbols in code always raise.
    assert rest.unlisted == [OnError.LOG, OnError.RAISE]
    assert rest.fetched == []


def test_fetch_follows_a_refreshing_table_and_reads_a_returning_symbol_again(
    *,
    live: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    rest: Recorder,
    drain: Callable[[], None],
    dh_to_pylist: Callable[[Table], list[Any]],
) -> None:
    rest.outcomes = {"AAPL": rows_of(massive_session, LAST_TRADE_SPEC, 1)}
    wl = live("AAPL")
    q = make(massive_session, LAST_TRADE_SPEC, symbols=wl)
    assert isinstance(q, FetchQuery)
    result = q.fetch(wait=True)
    assert result.is_refreshing
    assert result.size == 1  # the call-time read is in the result when the call returns
    assert [(names, thread) for names, _, thread in rest.fetched] == [
        (("AAPL",), threading.current_thread().name)
    ]
    assert result.attributes()["TableDescription"].endswith(
        "Follows its symbol table: each symbol's rows are Massive's answer as of its read."
    )
    rest.outcomes["MSFT"] = rows_of(massive_session, LAST_TRADE_SPEC, 2)
    wl.add(new_table([string_col("Symbol", ["MSFT"])]))
    until(lambda: len(dh_to_pylist(result)) == 3)
    assert " read_" in rest.fetched[1][2]
    until(lambda: symbol_reads(massive_session, drain, dh_to_pylist)["MSFT"]["State"] == "DONE")
    reads = symbol_reads(massive_session, drain, dh_to_pylist)
    # Each symbol's rows are Massive's answer as of its own read.
    assert reads["MSFT"]["ReadAt"] > reads["AAPL"]["ReadAt"]
    wl.delete(new_table([string_col("Symbol", ["MSFT"])]))
    until(lambda: len(dh_to_pylist(result)) == 1)
    wl.add(new_table([string_col("Symbol", ["MSFT"])]))
    until(lambda: len(rest.fetched) == 3)  # read again, with no reread option
    until(lambda: len(dh_to_pylist(result)) == 3)
    massive_session.release(result)
    follows(massive_session)


def test_a_late_outcome_of_a_re_added_symbol_is_dropped(
    *,
    live: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    rest: Recorder,
    dh_to_pylist: Callable[[Table], list[Any]],
) -> None:
    # MSFT's first read is held while its row is deleted and added again; that read's outcome
    # (5 rows) lands after the re-add was queued, so only the second read's 1 row is shown.
    gate = threading.Event()
    rest.gates["MSFT"] = [gate]
    rest.turns["MSFT"] = [
        rows_of(massive_session, LAST_TRADE_SPEC, 5),
        rows_of(massive_session, LAST_TRADE_SPEC, 1),
    ]
    wl = live()
    q = make(massive_session, LAST_TRADE_SPEC, symbols=wl)
    assert isinstance(q, FetchQuery)
    result = q.fetch(wait=True)
    wl.add(new_table([string_col("Symbol", ["MSFT"])]))
    until(lambda: len(rest.fetched) == 1)
    wl.delete(new_table([string_col("Symbol", ["MSFT"])]))
    wl.add(new_table([string_col("Symbol", ["MSFT"])]))
    gate.set()
    until(lambda: len(rest.fetched) == 2)
    until(lambda: len(dh_to_pylist(result)) == 1)
    massive_session.release(result)
    follows(massive_session)


def test_fetch_wait_false_returns_at_once_and_reads_in_the_background(
    *, live: Callable[..., InputTable], massive_session: dm.MassiveSession, rest: Recorder
) -> None:
    q = make(massive_session, LAST_TRADE_SPEC, symbols=live("AAPL"))
    assert isinstance(q, FetchQuery)
    result = q.fetch(wait=False)
    assert result.is_refreshing
    until(lambda: len(rest.fetched) == 1)
    assert " read_" in rest.fetched[0][2]
    assert rest.checks == [("fetch", LAST_TRADE_SPEC, rest.checks[0][2])]  # checked at the call
    massive_session.release(result)
    follows(massive_session)


def test_a_followed_fetch_raises_only_when_every_call_time_symbol_fails(
    *,
    live: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    rest: Recorder,
    caplog: pytest.LogCaptureFixture,
) -> None:
    refusal = dm.EntitlementError("HTTP 403: not in the plan")
    rest.outcomes = {"AAPL": SymbolFailure(refusal, FailureKind.PERMANENT)}
    q = make(massive_session, LAST_TRADE_SPEC, symbols=live("AAPL"))
    assert isinstance(q, FetchQuery)
    with pytest.raises(dm.EntitlementError, match="not in the plan"):
        q.fetch(wait=True)
    both = make(massive_session, LAST_TRADE_SPEC, symbols=live("AAPL", "MSFT"))
    assert isinstance(both, FetchQuery)
    with caplog.at_level(logging.WARNING, logger="deephaven_massive.queries"):
        result = both.fetch(wait=True)
    assert caplog.messages == [
        (
            "stocks.last_trade fetch: 1 of the 2 symbols of the call failed and are left out "
            "of the result for now; m.symbol_reads() shows each one (retrying, or failed): "
            "AAPL: HTTP 403: not in the plan"
        )
    ]
    rest.outcomes = {}  # the raised error's traceback holds the session
    massive_session.release(result)
    follows(massive_session)


def test_a_followed_fetch_whose_every_symbol_failed_is_retryable_only_when_each_is(
    *, live: Callable[..., Table], massive_session: dm.MassiveSession, rest: Recorder
) -> None:
    busy = dm.MassiveError("HTTP 503: busy", status=503)
    refusal = dm.EntitlementError("HTTP 403: not in the plan", status=403)
    rest.outcomes = {
        "AAPL": SymbolFailure(busy, FailureKind.TRANSIENT),
        "MSFT": SymbolFailure(busy, FailureKind.TRANSIENT),
    }
    both = make(massive_session, LAST_TRADE_SPEC, symbols=live("AAPL", "MSFT"))
    assert isinstance(both, FetchQuery)
    with pytest.raises(dm.MassiveError, match="busy") as retried:
        both.fetch(wait=True)
    assert retried.value.is_retryable
    rest.outcomes["MSFT"] = SymbolFailure(refusal, FailureKind.PERMANENT)
    with pytest.raises(dm.MassiveError, match="busy") as mixed:
        both.fetch(wait=True)
    # AAPL's error, the first, but calling again cannot help MSFT.
    assert (mixed.value.status, mixed.value.is_retryable) == (503, False)
    assert mixed.value.__cause__ is busy
    rest.outcomes = {}  # the raised errors' tracebacks hold the session
    follows(massive_session)


@pytest.mark.parametrize("setting", [OnError.LOG, OnError.IGNORE, OnError.RAISE])
def test_a_followed_fetch_s_unlisted_pair_follows_unlisted_symbols(
    *,
    live: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    rest: Recorder,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    setting: OnError,
) -> None:
    core = massive_session._core
    monkeypatch.setattr(core, "config", dataclasses.replace(core.config, unlisted_symbols=setting))
    error = UnlistedPairError("'DOT' cannot be split: Massive's reference data does not list it")
    rest.outcomes = {"DOT": SymbolFailure(error, FailureKind.PERMANENT)}
    q = make(massive_session, LAST_TRADE_SPEC, symbols=live("AAPL", "DOT"))
    assert isinstance(q, FetchQuery)
    with caplog.at_level(logging.WARNING, logger="deephaven_massive.queries"):
        if setting is OnError.RAISE:
            with pytest.raises(UnlistedPairError, match="does not list it"):
                q.fetch(wait=True)
            rest.outcomes = {}  # the raised error's traceback holds the session
            return
        result = q.fetch(wait=True)
    assert len(caplog.messages) == (1 if setting is OnError.LOG else 0)
    massive_session.release(result)
    follows(massive_session)


def test_a_followed_fetch_groups_symbols_only_where_a_request_names_many() -> None:
    batched = dataclasses.replace(LAST_TRADE_SPEC, symbols_per_request=250)
    assert _symbols_per_read(batched) == 250
    assert _symbols_per_read(LAST_TRADE_SPEC) == 1


def test_a_followed_snapshot_reads_its_symbols_in_one_request_and_splits_the_rows(
    *,
    live: Callable[..., InputTable],
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    dh_to_pylist: Callable[[Table], list[Any]],
) -> None:
    # Through the real REST transport: one ticker.any_of request names both symbols, and each
    # row goes to its symbol by its ticker.
    rows = [{"ticker": "AAPL", "type": "stocks"}, {"ticker": "MSFT", "type": "stocks"}]
    fake_rest.reply("/v3/snapshot", Reply(envelope(rows)))
    result = rest_session.snapshots(live("AAPL", "MSFT")).fetch(wait=True)
    assert sorted(r["Symbol"] for r in dh_to_pylist(result)) == ["AAPL", "MSFT"]
    ((path, query),) = fake_rest.get_queries()
    assert path == "/v3/snapshot"
    assert query["ticker.any_of"] == ["AAPL,MSFT"]
    rest_session.release(result)
    follows(rest_session)


def test_a_followed_fetch_sends_its_limit_on_its_request(
    *, live: Callable[..., InputTable], rest_session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    # `limit` is Massive's own cap on a request's rows, sent as the caller gave it.
    fake_rest.reply("/v3/snapshot", Reply(envelope([{"ticker": "AAPL", "type": "stocks"}])))
    result = rest_session.snapshots(live("AAPL"), limit=1).fetch(wait=True)
    ((_, query),) = fake_rest.get_queries()
    assert (query["ticker.any_of"], query["limit"]) == (["AAPL"], ["1"])
    rest_session.release(result)
    follows(rest_session)


def test_a_fetch_of_an_empty_refreshing_table_fills_as_symbols_are_added(
    *,
    live: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    rest: Recorder,
    dh_to_pylist: Callable[[Table], list[Any]],
) -> None:
    rest.outcomes = {"AAPL": rows_of(massive_session, LAST_TRADE_SPEC, 1)}
    wl = live()
    q = make(massive_session, LAST_TRADE_SPEC, symbols=wl)
    assert isinstance(q, FetchQuery)
    result = q.fetch(wait=True)
    assert result.is_refreshing
    assert result.size == 0
    assert (rest.fetched, rest.checks) == ([], [])  # the first read checks the parameters
    wl.add(new_table([string_col("Symbol", ["AAPL"])]))
    until(lambda: len(dh_to_pylist(result)) == 1)
    massive_session.release(result)
    follows(massive_session)


def test_a_followed_fetch_lists_an_article_under_each_symbol_it_answers(
    *,
    live: Callable[..., InputTable],
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    dh_to_pylist: Callable[[Table], list[Any]],
) -> None:
    # Through the real REST transport: each ticker is read in a request of its own, and an
    # article Massive returns for both is the data listed by symbol, so it is listed twice;
    # deleting one ticker leaves the other's copy.
    shared = {
        "id": "both",
        "published_utc": "2024-06-24T18:00:00Z",
        "tickers": ["AAPL", "MSFT"],
        "title": "Article both",
        "article_url": "https://example.com/both",
        "publisher": {"name": "Example"},
    }
    for ticker in ("AAPL", "MSFT"):
        fake_rest.reply(f"/v2/reference/news?limit=1000&ticker={ticker}", Reply(envelope([shared])))
    wl = live("AAPL", "MSFT")
    result = rest_session.reference.news(symbols=wl).fetch(wait=True)
    assert [r["Id"] for r in dh_to_pylist(result)] == ["both", "both"]
    assert sorted(query["ticker"][0] for _, query in fake_rest.get_queries()) == ["AAPL", "MSFT"]
    wl.delete(new_table([string_col("Symbol", ["AAPL"])]))
    until(lambda: [r["Id"] for r in dh_to_pylist(result)] == ["both"])
    rest_session.release(result)
    follows(rest_session)


def test_a_followed_read_s_patience_starts_when_its_read_does(
    live: Callable[..., InputTable], *, fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    # One read thread: MSFT waits in the queue while AAPL is read. Its patience for rate
    # limits begins only when its own read begins, so the time it queued never counts.
    events: list[str] = []
    lock = threading.Lock()
    patience = rest_module.RestReader.patience

    def recorded(reader: rest_module.RestReader) -> Patience:
        with lock:
            events.append("patience")
        return patience(reader)

    def heard(received: Received) -> None:
        with lock:
            events.append(received.target.split("?")[0].rsplit("/", 1)[-1])

    monkeypatch.setattr(rest_module.RestReader, "patience", recorded)
    fake_rest.on_request(heard)
    fake_rest.reply_default(Reply(envelope([])))
    config = dm.MassiveConfig(api_key="test-key", rest_base_url=fake_rest.url, max_concurrency=1)
    with dm.MassiveSession(config) as m:
        result = m.stocks.trades(live("AAPL", "MSFT")).history(
            "2025-03-03", "2025-03-03", wait=False
        )
        until(lambda: events.count("patience") == 2 and len(events) == 4)
        m.release(result)
    assert events == ["patience", "AAPL", "patience", "MSFT"]


def test_a_followed_range_no_row_can_lie_in_checks_nothing(
    live: Callable[..., InputTable], *, massive_session: dm.MassiveSession, rest: Recorder
) -> None:
    moment = datetime(2025, 3, 3, 15, tzinfo=UTC)
    result = trades(massive_session, live("AAPL")).history(
        moment, moment, wait=True
    )  # an empty range
    assert rest.checks == []  # nothing can be asked: no request, no parameter to check
    massive_session.release(result)
    follows(massive_session)


@pytest.fixture
def live_codes(dh_input_table: Callable[..., InputTable]) -> Callable[..., InputTable]:
    # Makes a refreshing, keyed table of futures product codes, as `m.futures.products()` rows
    # carry them.
    def make(*codes: str) -> InputTable:
        table = dh_input_table({"ProductCode": dtypes.string}, key_cols=["ProductCode"])
        if codes:
            table.add(new_table([string_col("ProductCode", list(codes))]))
        return table

    return make


def test_a_static_table_of_underlyings_reads_each_chain_once(
    *, massive_session: dm.MassiveSession, rest: Recorder
) -> None:
    rest.outcomes = {
        "AAPL": rows_of(massive_session, CHAIN, 2),
        "MSFT": rows_of(massive_session, CHAIN, 1),
    }
    underlyings = new_table([string_col("Symbol", ["MSFT", "AAPL", "MSFT"])])
    q = massive_session.options.chain(underlyings)
    assert repr(q) == "m.options.chain(<table>)"
    result = q.fetch(wait=True)
    assert not result.is_refreshing
    assert result.size == 3
    [(values, request, _)] = rest.valued
    assert values == ("AAPL", "MSFT")
    assert request.value_table is not None
    assert request.for_value("AAPL").params == (("underlyingAsset", "AAPL"),)


def test_a_static_table_of_values_fails_when_one_value_fails(
    *, massive_session: dm.MassiveSession, rest: Recorder
) -> None:
    refusal = dm.EntitlementError("HTTP 403: not in the plan")
    rest.outcomes = {"NQ": SymbolFailure(refusal, FailureKind.PERMANENT)}
    codes = new_table([string_col("ProductCode", ["ES", "NQ"])])
    with pytest.raises(dm.EntitlementError, match="not in the plan"):
        massive_session.futures.products(product_code=codes).fetch(wait=True)
    rest.outcomes = {}  # the raised error's traceback holds the session


def test_a_static_table_of_values_reads_in_the_background_with_wait_false(
    *, massive_session: dm.MassiveSession, rest: Recorder
) -> None:
    codes = new_table([string_col("ProductCode", ["ES"])])
    result = massive_session.futures.products(product_code=codes).fetch(wait=False)
    assert result.is_refreshing
    [(_, _, checked)] = rest.checks  # checked at the call, with the table's first value
    assert ("product_code", "ES") in checked.params
    until(lambda: len(rest.valued) == 1)
    massive_session.release(result)


def test_a_table_of_values_with_no_value_it_takes_is_refused(
    *, massive_session: dm.MassiveSession, rest: Recorder
) -> None:
    empty = new_table([string_col("ProductCode", [None])])
    with pytest.raises(ValueError, match=r"the product_code table holds no values$"):
        massive_session.futures.products(product_code=empty).fetch(wait=True)
    options = new_table([string_col("Symbol", ["O:AAPL250117C00150000"])])
    q = massive_session.options.chain(options)
    with pytest.raises(ValueError, match="no row of the underlying table is a value this dataset"):
        q.fetch(wait=True)
    with pytest.raises(ValueError, match="no row of the underlying table is a value this dataset"):
        q.poll(5)
    assert rest.valued == []


def test_fetch_follows_a_refreshing_table_of_product_codes(
    *,
    live_codes: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    rest: Recorder,
    drain: Callable[[], None],
    dh_to_pylist: Callable[[Table], list[Any]],
) -> None:
    rest.outcomes = {"ES": rows_of(massive_session, futures.PRODUCTS, 1)}
    codes = live_codes("ES")
    result = massive_session.futures.products(product_code=codes).fetch(wait=True)
    assert result.is_refreshing
    assert result.size == 1  # the call-time code is in the result when the call returns
    assert result.attributes()["TableDescription"].endswith(
        "Follows its table of product_code values: each value's rows are Massive's answer as of "
        "its read."
    )
    rest.outcomes["NQ"] = rows_of(massive_session, futures.PRODUCTS, 2)
    codes.add(new_table([string_col("ProductCode", ["NQ"])]))
    until(lambda: len(dh_to_pylist(result)) == 3)
    assert [values for values, _, _ in rest.valued] == [("ES",), ("NQ",)]
    assert " read_" in rest.valued[1][2]
    assert set(symbol_reads(massive_session, drain, dh_to_pylist)) == {"ES", "NQ"}
    codes.delete(new_table([string_col("ProductCode", ["ES"])]))
    until(lambda: len(dh_to_pylist(result)) == 2)
    massive_session.release(result)
    follows(massive_session)


def test_fetch_follows_a_refreshing_table_of_underlyings_with_wait_false(
    *, live: Callable[..., InputTable], massive_session: dm.MassiveSession, rest: Recorder
) -> None:
    result = massive_session.options.chain(live()).fetch(wait=False)
    assert result.is_refreshing
    assert (rest.valued, rest.checks) == ([], [])  # nothing to read or check yet
    assert "Follows its table of underlying values" in result.attributes()["TableDescription"]
    massive_session.release(result)
    follows(massive_session)
    result = massive_session.options.chain(live("AAPL")).fetch(wait=False)
    until(lambda: len(rest.valued) == 1)
    [(_, _, checked)] = rest.checks
    assert ("underlyingAsset", "AAPL") in checked.params
    massive_session.release(result)
    follows(massive_session)


def test_fetch_follows_one_refreshing_table_at_a_time(
    *,
    live: Callable[..., InputTable],
    live_codes: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    rest: Recorder,
) -> None:
    q = massive_session.futures.snapshots(live("ESZ6"), product_code=live_codes("ES"))
    with pytest.raises(
        ValueError,
        match=r"both symbols and product_code are refreshing tables\. Use poll\(interval\), "
        r"which reads both at every cycle, or pass one of them as a static snapshot, such as "
        r"product_code=table\.snapshot\(\)$",
    ):
        q.fetch(wait=True)
    # A static symbol table beside it is read once, and the codes are followed.
    contracts = new_table([string_col("Symbol", ["ESZ6"])])
    result = massive_session.futures.snapshots(contracts, product_code=live_codes("ES")).fetch(
        wait=True
    )
    [(_, request, _)] = rest.valued
    assert request.symbols == ("ESZ6",)
    massive_session.release(result)
    follows(massive_session)


def test_poll_reads_a_table_of_values_at_its_call(
    *, live_codes: Callable[..., InputTable], massive_session: dm.MassiveSession, rest: Recorder
) -> None:
    codes = live_codes("ES")
    massive_session.futures.market_status(product_code=codes).poll(5)
    [(verb, _, request, _)] = rest.calls
    assert verb == "poll"
    assert request.value_table is not None
    assert request.value_table.table is codes
