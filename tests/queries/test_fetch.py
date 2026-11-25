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
    AssetClass,
    PollMode,
)
from deephaven_massive._ingest import rest as rest_module
from deephaven_massive._massive import Patience
from deephaven_massive._tables.execution_context import entered
from deephaven_massive._tables.transports import (
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
    last_trade,
    make,
    news,
    strict,
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
        "m.economy.inflation().poll() needs a start or a limit. Without one, every cycle "
        "re-reads Massive's whole archive. A start is fixed when the query is built, so the "
        "window it bounds grows every day the poll runs. For the latest rows, pass "
        "m.economy.inflation() a limit with sort='date', order=dm.Order.DESC, then call poll()."
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


@pytest.mark.parametrize(
    ("method", "newest"),
    [
        (lambda m, **kw: m.reference.news(**kw), {"order": dm.Order.DESC}),
        (
            lambda m, **kw: m.partners.benzinga.news(**kw),
            {"sort": "published", "order": dm.Order.DESC},
        ),
        (
            lambda m, **kw: m.options.unusual_trades(**kw),
            {"sort": "sip_timestamp", "order": dm.Order.DESC},
        ),
    ],
    ids=["reference.news", "partners.benzinga.news", "options.unusual_trades"],
)
def test_each_feed_polls_as_its_refusal_advises(
    *,
    massive_session: dm.MassiveSession,
    rest: Recorder,
    method: Callable[..., FetchQuery],
    newest: dict[str, object],
) -> None:
    # The fix the refusal of a poll without a start or a limit names, on every feed.
    named = ", ".join(
        f"{key}=dm.Order.DESC" if key == "order" else f"{key}={value!r}"
        for key, value in newest.items()
    )
    with pytest.raises(ValueError, match=re.escape(f"a limit with {named}, then call poll().")):
        method(massive_session).poll(60)
    method(massive_session, limit=50, **newest).poll(60)
    assert [c[0] for c in rest.calls] == ["poll"]


def test_a_feed_with_a_limit_polls_its_latest_rows(
    massive_session: dm.MassiveSession, rest: Recorder
) -> None:
    # The "latest headlines" poll: each cycle reads at most `limit` rows, never the archive.
    news(massive_session, limit=50).poll(60)
    assert [c[0] for c in rest.calls] == ["poll"]


@pytest.mark.parametrize(
    ("interval", "refusal"),
    [
        (0, "interval=0 is not a positive, finite number."),
        (-1, "interval=-1 is not a positive, finite number."),
        (float("inf"), "interval=inf is not a positive, finite number."),
        (float("nan"), "interval=nan is not a positive, finite number."),
        (
            1e300,
            (
                "interval=1e+300 is longer than a timedelta can hold. Pass a number of seconds, "
                "such as 60."
            ),
        ),
        (10**400, "interval is an int too large to convert to a float. Pass a smaller number."),
    ],
)
def test_poll_interval_values(
    *, massive_session: dm.MassiveSession, rest: Recorder, interval: object, refusal: str
) -> None:
    with pytest.raises(ValueError, match=f"^{re.escape(refusal)}$"):
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
    message = f"interval={interval!r} is below the least 1 s. Pass 1 or more."
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
        TypeError,
        match=r"^mode must be dm\.PollMode, such as dm\.PollMode\.UPSERT, not 'upsert'$",
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


def rows_of(m: dm.MassiveSession, spec: DatasetSpec, count: int) -> SymbolRows:
    # A symbol's static rows, as a read returns them: `count` rows of nulls.
    schema = m._core.dataset_schema(spec)
    with entered(m._core.exec_ctx):
        return SymbolRows(dh_arrow.to_table(schema.to_arrow([{}] * count)))


def test_a_static_fetch_is_unchanged(*, massive_session: dm.MassiveSession, rest: Recorder) -> None:
    static = new_table([string_col("Symbol", ["AAPL"])])
    q = make(massive_session, LAST_TRADE_SPEC, symbols=static)
    assert isinstance(q, FetchQuery)
    assert not q.fetch(wait=True).is_refreshing
    assert not last_trade(massive_session).fetch(wait=True).is_refreshing
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
    # The call waited for its first read, so it waits out pauses as the caller's; the later
    # reads, which no caller waits for, get a stop token and wait out max_outage.
    assert rest.fetch_stopped == [False, True, True]
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
    # Checked at the call: every path the table could reach.
    assert [check[:2] for check in rest.checks] == [("fetch", LAST_TRADE_SPEC)]
    massive_session.release(result)
    follows(massive_session)


def test_a_followed_fetch_raises_only_when_every_call_time_symbol_fails(
    *,
    live: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    rest: Recorder,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Massive's answer to the read of AAPL, as the REST client words it.
    refusal = dm.EntitlementError(
        "Massive answered GET /v2/last/trade/AAPL on api.massive.com with HTTP 403: "
        '"You are not entitled to this data".',
        status=403,
    )
    rest.outcomes = {"AAPL": SymbolFailure(refusal)}
    q = make(massive_session, LAST_TRADE_SPEC, symbols=live("AAPL"))
    assert isinstance(q, FetchQuery)
    with pytest.raises(dm.EntitlementError, match="not entitled"):
        q.fetch(wait=True)
    both = make(massive_session, LAST_TRADE_SPEC, symbols=live("AAPL", "MSFT"))
    assert isinstance(both, FetchQuery)
    with caplog.at_level(logging.WARNING, logger="deephaven_massive.queries"):
        result = both.fetch(wait=True)
    assert caplog.messages == [
        (
            "stocks.last_trade fetch: 1 of the 2 symbols failed and are left out of the result "
            "for now. AAPL: Massive answered GET /v2/last/trade/AAPL on api.massive.com with "
            'HTTP 403: "You are not entitled to this data". Check m.symbol_reads() for whether '
            "each one is retrying or has failed."
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
        "AAPL": SymbolFailure(busy),
        "MSFT": SymbolFailure(busy),
    }
    both = make(massive_session, LAST_TRADE_SPEC, symbols=live("AAPL", "MSFT"))
    assert isinstance(both, FetchQuery)
    with pytest.raises(dm.MassiveError, match="busy") as retried:
        both.fetch(wait=True)
    assert retried.value.is_retryable
    rest.outcomes["MSFT"] = SymbolFailure(refusal)
    with pytest.raises(dm.MassiveError, match="busy") as mixed:
        both.fetch(wait=True)
    # AAPL's error, the first, but calling again cannot help MSFT.
    assert (mixed.value.status, mixed.value.is_retryable) == (503, False)
    assert mixed.value.__cause__ is busy
    rest.outcomes = {}  # the raised errors' tracebacks hold the session
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
    assert rest.fetched == []
    # The call checks every path a symbol of the table could reach, though it holds none.
    assert [check[:2] for check in rest.checks] == [("fetch", LAST_TRADE_SPEC)]
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
    rest.outcomes = {"NQ": SymbolFailure(refusal)}
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
    nothing = (
        "m.futures.products(product_code=<table>): The product_code table holds no value. Pass "
        "a table whose ProductCode column holds at least one value."
    )
    with pytest.raises(ValueError, match=f"^{re.escape(nothing)}$"):
        massive_session.futures.products(product_code=empty).fetch(wait=True)
    options = new_table([string_col("Symbol", ["O:AAPL250117C00150000", "X:BTCUSD"])])
    q = massive_session.options.chain(options)
    skipped = (
        "m.options.chain(<table>): No row of the underlying table is a value this dataset takes, "
        "so the result would read nothing. The table holds 'O:AAPL250117C00150000' and "
        "'X:BTCUSD'. Fix or delete those rows, or report them at "
        "https://github.com/chipkent/deephaven-massive/issues if Massive itself writes them."
    )
    with pytest.raises(ValueError, match=f"^{re.escape(skipped)}$"):
        q.fetch(wait=True)
    with pytest.raises(ValueError, match=f"^{re.escape(skipped)}$"):
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
    refusal = (
        f"{q!r}.fetch() follows one refreshing table, and both symbols and product_code are "
        "refreshing tables. Call poll(60) instead, which reads both every 60 s, or pass one of "
        "them as a snapshot, such as product_code=table.snapshot()."
    )
    with pytest.raises(ValueError, match=f"^{re.escape(refusal)}$"):
        q.fetch(wait=True)
    # Both fixes the refusal names work.
    q.poll(60)
    assert [c[0] for c in rest.calls] == ["poll"]
    codes = live_codes("ES")
    snapshot = massive_session.futures.snapshots(live("ESZ6"), product_code=codes.snapshot())
    snapshot.fetch(wait=True)  # static: read once, as the refusal says
    rest.valued.clear()
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


def test_a_followed_fetch_of_a_pair_massive_does_not_list_ends_done_with_no_rows(
    *,
    live: Callable[..., Table],
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    dh_to_pylist: Callable[[Table], list[Any]],
) -> None:
    # Through the real REST transport: Massive's crypto ticker list holds only BTC-USD, so
    # DOT-USD has no data, as a ticker Massive answers with nothing has: DONE, with no rows.
    listed = {"ticker": "X:BTCUSD", "base_currency_symbol": "BTC", "currency_symbol": "USD"}
    fake_rest.reply("/v3/reference/tickers", Reply(envelope([listed])), Reply(envelope([])))
    last = {"status": "OK", "symbol": "BTC-USD", "last": {"price": 1.0, "timestamp": 1741000000000}}
    fake_rest.reply("/v1/last/crypto/BTC/USD", Reply(last))
    result = rest_session.crypto.last_trade(live("X:BTCUSD", "X:DOTUSD")).fetch(wait=True)
    assert [r["Symbol"] for r in dh_to_pylist(result)] == ["X:BTCUSD"]

    def states() -> dict[str, tuple[str, int]]:
        rows = dh_to_pylist(rest_session.symbol_reads())
        return {r["Symbol"]: (r["State"], r["Rows"]) for r in rows}

    until(lambda: states().get("X:DOTUSD") == ("DONE", 0))
    rest_session.release(result)
    follows(rest_session)


def test_a_followed_fetch_names_a_listed_pair_whose_ticker_it_can_t_read_in_symbol_reads(
    *,
    live: Callable[..., Table],
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    dh_to_pylist: Callable[[Table], list[Any]],
) -> None:
    # Massive's crypto ticker list gives X:DOTUSD the currencies DOT and USDT, which don't make
    # up its ticker: no request is sent for it, and its m.symbol_reads() row says why, as a
    # stream's and a poll's Error do. A pair the list reads is read with no Error.
    listed = {"ticker": "X:BTCUSD", "base_currency_symbol": "BTC", "currency_symbol": "USD"}
    unjoined = {"ticker": "X:DOTUSD", "base_currency_symbol": "DOT", "currency_symbol": "USDT"}
    fake_rest.reply(
        "/v3/reference/tickers", Reply(envelope([listed, unjoined])), Reply(envelope([]))
    )
    last = {"status": "OK", "symbol": "BTC-USD", "last": {"price": 1.0, "timestamp": 1741000000000}}
    fake_rest.reply("/v1/last/crypto/BTC/USD", Reply(last))
    result = rest_session.crypto.last_trade(live("X:BTCUSD", "X:DOTUSD")).fetch(wait=True)
    assert [r["Symbol"] for r in dh_to_pylist(result)] == ["X:BTCUSD"]

    def reads() -> dict[str, tuple[str, int, str | None]]:
        rows = dh_to_pylist(rest_session.symbol_reads())
        return {r["Symbol"]: (r["State"], r["Rows"], r["Error"]) for r in rows}

    until(lambda: reads().get("X:DOTUSD", ("", 0, None))[0] == "DONE")
    state, rows, error = reads()["X:DOTUSD"]
    assert (state, rows) == ("DONE", 0)
    assert error == (
        "skipped: Massive's reference data gives the currencies 'DOT' and 'USDT', which do not "
        "join to it, so the pair gets no rows"
    )
    assert reads()["X:BTCUSD"] == ("DONE", 1, None)
    assert [p for p, _ in fake_rest.get_queries() if p != "/v3/reference/tickers"] == [
        "/v1/last/crypto/BTC/USD"
    ]
    rest_session.release(result)
    follows(rest_session)


RAISES = dm.ErrorPolicy(noncanonical_symbols=dm.OnError.RAISE)
"""A call's policy that fails on a symbol-table row that is not Massive's ticker."""


@pytest.mark.parametrize("refreshing", [False, True], ids=["static", "refreshing"])
def test_fetch_and_poll_refuse_a_bad_symbol_row_when_the_call_raises_and_the_session_logs(
    refreshing: bool,
    *,
    live: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    rest: Recorder,
    refusal: Callable[..., str],
) -> None:
    rows = ["AAPL", "msft"]
    table = live(*rows) if refreshing else new_table([string_col("Symbol", rows)])
    q = massive_session.stocks.last_trade(table, on_error=RAISES)
    refused = (
        "^"
        + re.escape(
            f"stocks.last_trade: A symbol table holds 'msft'. {refusal(AssetClass.STOCKS, 'msft')} "
            "Fix the row, or convert the table with dm.canonicalize_symbols(table, "
            "dm.AssetClass.STOCKS, convention=dm.SymbolConvention.YAHOO), naming the convention "
            "your tickers come from."
        )
        + "$"
    )
    with pytest.raises(ValueError, match=refused):
        q.fetch(wait=True)
    with pytest.raises(ValueError, match=refused):
        q.poll(5)
    assert (rest.calls, rest.fetched) == ([], [])


@pytest.mark.parametrize("refreshing", [False, True], ids=["static", "refreshing"])
def test_fetch_and_poll_refuse_a_bad_value_row_when_the_call_raises_and_the_session_logs(
    refreshing: bool,
    *,
    live_codes: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    rest: Recorder,
) -> None:
    rows = ["ES", " CL"]
    codes = live_codes(*rows) if refreshing else new_table([string_col("ProductCode", rows)])
    q = massive_session.futures.products(product_code=codes, on_error=RAISES)
    refused = (
        "^"
        + re.escape(
            "futures.products: A symbol table holds ' CL', which has surrounding whitespace, "
            "and Massive never writes a ticker that way. Fix the row to the value Massive "
            "lists, or delete it."
        )
        + "$"
    )
    with pytest.raises(ValueError, match=refused):
        q.fetch(wait=True)
    with pytest.raises(ValueError, match=refused):
        q.poll(5)
    assert (rest.calls, rest.valued) == ([], [])


def test_fetch_skips_a_bad_row_when_the_call_logs_and_the_session_raises(
    *,
    massive_session: dm.MassiveSession,
    rest: Recorder,
    monkeypatch: pytest.MonkeyPatch,
    problem_rows: Callable[[], list[dict[str, Any]]],
) -> None:
    strict(massive_session, monkeypatch)
    logs = dm.ErrorPolicy(noncanonical_symbols=dm.OnError.LOG)
    symbols = new_table([string_col("Symbol", ["AAPL", "msft"])])
    massive_session.stocks.last_trade(symbols, on_error=logs).fetch(wait=True)
    codes = new_table([string_col("ProductCode", ["ES", " CL"])])
    massive_session.futures.products(product_code=codes, on_error=logs).fetch(wait=True)
    [(_, _, request, _)] = rest.calls
    assert request.symbols == ("AAPL",)
    [(values, _, _)] = rest.valued
    assert values == ("ES",)
    # Each skipped value is a problem of its call, with the call's setting.
    assert [
        (r["Cause"], r["Action"], r["Symbol"], r["Field"], r["Policy"]) for r in problem_rows()
    ] == [
        ("noncanonical_symbols", "SKIPPED", "msft", None, "log"),
        ("noncanonical_symbols", "SKIPPED", " CL", "ProductCode", "log"),
    ]


def test_a_one_time_read_lists_each_value_canonicalize_symbols_refused(
    *,
    massive_session: dm.MassiveSession,
    rest: Recorder,
    problem_rows: Callable[[], list[dict[str, Any]]],
) -> None:
    # The converted table holds a null for each refused value, which the read skips like any
    # null; the call still lists the value once, with the converter's setting, as a live read
    # would while the row holds it.
    symbols = dm.canonicalize_symbols(
        new_table([string_col("Symbol", ["AAPL", "no pe!"])]), dm.AssetClass.STOCKS
    )
    massive_session.stocks.last_trade(symbols).fetch(wait=True)
    underlyings = dm.canonicalize_symbols(
        new_table([string_col("Symbol", ["AAPL", "bad one"])]),
        dm.AssetClass.STOCKS,
        on_error=dm.OnError.IGNORE,
    )
    massive_session.options.chain(underlyings).fetch(wait=True)
    assert [request.symbols for _, _, request, _ in rest.calls] == [("AAPL",)]
    assert [values for values, _, _ in rest.valued] == [("AAPL",)]
    rows = [
        (r["Cause"], r["Action"], r["Symbol"], r["Field"], r["Policy"], r["EndedAt"])
        for r in problem_rows()
    ]
    assert rows == [
        ("noncanonical_symbols", "SKIPPED", "no pe!", None, "log", None),
        ("noncanonical_symbols", "SKIPPED", "bad one", "Symbol", "ignore", None),
    ]
    # The converter's reason alone, without its fix: the row's Cause leads to the fix.
    assert [r["Detail"] for r in problem_rows()] == [
        "canonicalize_symbols refused it: 'no pe!' is not a Massive stock ticker.",
        "canonicalize_symbols refused it: 'bad one' is not a Massive stock ticker.",
    ]


def test_a_followed_fetch_of_underlyings_names_their_column_in_field(
    *,
    live: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    rest: Recorder,
    problem_rows: Callable[[], list[dict[str, Any]]],
) -> None:
    # As a poll() of the same table does: a value the read skips, and one the converter
    # refused, each name the table's column.
    raw = live("AAPL", "no pe!")
    result = massive_session.options.chain(raw).fetch(wait=True)
    source = live("MSFT", "bad one")
    converted = dm.canonicalize_symbols(source, dm.AssetClass.STOCKS)
    other = massive_session.options.chain(converted).fetch(wait=True)

    def fields() -> list[tuple[object, ...]]:
        return sorted(
            (r["Symbol"], r["Field"])
            for r in problem_rows()
            if r["Cause"] == "noncanonical_symbols"
        )

    until(lambda: fields() == [("bad one", "Symbol"), ("no pe!", "Symbol")])
    massive_session.release(result)
    massive_session.release(other)
    follows(massive_session)


def test_a_followed_fetch_fails_on_a_late_bad_row_when_the_call_raises_and_the_session_logs(
    *,
    live: Callable[..., InputTable],
    live_codes: Callable[..., InputTable],
    massive_session: dm.MassiveSession,
    rest: Recorder,
) -> None:
    watchlist = live("AAPL")
    by_symbol = massive_session.stocks.last_trade(watchlist, on_error=RAISES).fetch(wait=True)
    codes = live_codes("ES")
    by_value = massive_session.futures.products(product_code=codes, on_error=RAISES).fetch(
        wait=True
    )
    watchlist.add(new_table([string_col("Symbol", ["msft"])]))
    codes.add(new_table([string_col("ProductCode", [" CL"])]))
    until(lambda: bool(by_symbol.j_table.isFailed()))
    until(lambda: bool(by_value.j_table.isFailed()))
    follows(massive_session)
