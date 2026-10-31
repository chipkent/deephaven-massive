"""Tests for the WebSocket transport: the reconcile loop, connections, and live streams.

`Subscriptions` is tested alone, with a recording sender and a manual timer queue, so every step
of the reconcile loop is deterministic. Connections and streams are tested end to end, on the
`ws_session` fixture (`tests/_fakes/ws_session.py`): a real session whose `WsConnections`
reaches the fake Massive server through real `WsClient`s, with every client callback recorded
so a test waits for exactly the event it needs. Flushes are explicit (`WsSession.flush`): the
flush interval is an hour unless a test needs the flush thread itself.
"""

from __future__ import annotations

import dataclasses
import functools
import gc
import importlib
import json
import logging
import re
import threading
import time
import weakref
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Protocol, cast

import pytest
from deephaven import agg, dtypes, input_table, new_table, update_graph
from deephaven.column import string_col

import deephaven_massive as dm
from deephaven_massive import _massive
from deephaven_massive._datasets.catalog import DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._datasets.request import Request
from deephaven_massive._datasets.row_times import ISSUES_URL
from deephaven_massive._enums import AssetClass, ChannelFamily, NotBars, Recency
from deephaven_massive._errors import LIBRARY_BUG
from deephaven_massive._ingest import futures_calendar, host_decision, ws, ws_connection
from deephaven_massive._massive import Status, StatusKind, Transport, WsState
from deephaven_massive._tables import poll
from deephaven_massive._tables.diagnostics import ConnectionState, StreamUpdate
from deephaven_massive._tables.execution_context import entered
from deephaven_massive._tables.liveness import new_scope, release_scope
from deephaven_massive._tables.stream import open_stream_tables
from deephaven_massive._tables.transports import HostDecision
from deephaven_massive._tables.watchlist import SymbolWatch
from tests._fakes.clock import FakeClock
from tests._fakes.ws_session import REALTIME, TIMEOUT

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Mapping
    from typing import TypeAlias

    import pyarrow as pa
    from deephaven.table import Table
    from deephaven.table_factory import InputTable

    from deephaven_massive._query import StreamQuery
    from deephaven_massive._tables.session_core import SessionCore
    from tests._fakes.ws import FakeMassiveServer, TradeFeed
    from tests._fakes.ws_session import WsSession


# Imported, not taken from a fixture: no shared fixture provides the manual clock that
# `poll.open_poll` needs for the thread-name test's poller, which must not cycle on its own.

# Plain constants of the ws_session, which no fixture can hand over; sessions come from fixtures.


RowsWhen: TypeAlias = (
    "Callable[[Table, Callable[[list[dict[str, Any]]], bool]], list[dict[str, Any]]]"
)
ToPylist: TypeAlias = "Callable[[Table], list[dict[str, Any]]]"
UserTables: TypeAlias = "Callable[[Table, str, str], list[Table]]"
FailuresHeard: TypeAlias = "Callable[[Table], Callable[[], int]]"
Opener: TypeAlias = "Callable[[WsSession, list[Table], InputTable | None], Live]"
W = Transport.WS
_JAVA_SYSTEM = cast("Any", importlib.import_module("jpy")).get_type("java.lang.System")


# --- The datasets the end-to-end tests stream -------------------------------------------------

STOCK_KEYS = SourceKeys(symbol=KeySpec("Ticker"), time=KeySpec("SipTimestamp"))
TRADES = DatasetSpec(
    name="test.trades",
    summary="A test dataset.",
    asset_class=AssetClass.STOCKS,
    channels={NotBars.ANY: "T"},
    keys={(W, "stocks/T"): STOCK_KEYS},
)
QUOTES = DatasetSpec(
    name="test.quotes",
    summary="A test dataset.",
    asset_class=AssetClass.STOCKS,
    channels={NotBars.ANY: "Q"},
    keys={(W, "stocks/Q"): STOCK_KEYS},
)
FOREX = DatasetSpec(
    name="test.forex",
    summary="A test dataset.",
    asset_class=AssetClass.FOREX,
    channels={NotBars.ANY: "C"},
    keys={(W, "forex/C"): SourceKeys(KeySpec("Ticker"), KeySpec("ParticipantTimestamp"))},
)
FOREX_FMV = DatasetSpec(
    name="test.forex_fmv",
    summary="A test dataset.",
    asset_class=AssetClass.FOREX,
    channels={NotBars.ANY: "FMV"},
    family=ChannelFamily.FMV,
    keys={(W, "forex/FMV"): SourceKeys(KeySpec("Ticker"))},
)
CRYPTO = DatasetSpec(
    name="test.crypto",
    summary="A test dataset.",
    asset_class=AssetClass.CRYPTO,
    channels={NotBars.ANY: "XT"},
    keys={(W, "crypto/XT"): SourceKeys(KeySpec("Pair"), KeySpec("ParticipantTimestamp"))},
)
OPTIONS = DatasetSpec(
    name="test.options",
    summary="A test dataset.",
    asset_class=AssetClass.OPTIONS,
    channels={NotBars.ANY: "T"},
    keys={(W, "options/T"): SourceKeys(KeySpec("Ticker"), KeySpec("SipTimestamp"))},
)
OPTION_QUOTES = DatasetSpec(
    name="test.option_quotes",
    summary="A test dataset.",
    asset_class=AssetClass.OPTIONS,
    channels={NotBars.ANY: "Q"},
    keys={(W, "options/Q"): SourceKeys(KeySpec("Ticker"), KeySpec("SipTimestamp"))},
)
FUTURES = DatasetSpec(
    name="test.futures",
    summary="A test dataset.",
    asset_class=AssetClass.FUTURES,
    channels={NotBars.ANY: "T"},
    keys={(W, "futures/T"): SourceKeys(KeySpec("Ticker"), KeySpec("Timestamp"))},
)


def trade(symbol: str, price: float = 1.0, **fields: object) -> dict[str, object]:
    row: dict[str, object] = {
        "ev": "T",
        "sym": symbol,
        "x": 4,
        "i": "1",
        "p": price,
        "q": 1,
        "t": 1_700_000_000_000,
        "s": 100,
        "z": 3,
        "ds": "100",
    }
    row.update(fields)
    return row


def wait_for(condition: Callable[[], bool]) -> None:
    # Bounded wait for an update-graph cycle to apply a change.
    pause = threading.Event()  # never set; only paces the loop
    for _ in range(300):
        if condition():
            return
        pause.wait(0.02)
    assert condition()


def settle(ws_session: WsSession) -> None:
    # Run what each live connection's loop has queued: a status's row write runs at the end of
    # its loop turn, after the status callback a test waits on, so a diagnostics read settles
    # first to see it. A connection whose loop has ended has nothing queued and is skipped.
    with ws_session.connections.group.lock:
        connections = list(ws_session.connections.group.connections.values())
    for connection in connections:
        done = threading.Event()
        if connection.post(done.set):
            assert done.wait(TIMEOUT)


STALL = (
    r"connection down since \d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ; no data; waiting for a WebSocket "
    r"slot on socket\.test/options: another connection with this API key, perhaps from another "
    r"process, holds it; the plan allows one connection per cluster"
)
"""A pattern of the `Error` of an options stream whose connection waits for a slot."""


def streams_of(
    ws_session: WsSession, dh_to_pylist: Callable[[Table], list[dict[str, Any]]]
) -> list[dict[str, Any]]:
    settle(ws_session)
    assert ws_session.session._core.diagnostics.barrier().wait(TIMEOUT)
    return sorted(dh_to_pylist(ws_session.session.streams()), key=lambda r: r["StreamId"])


def connections_of(
    ws_session: WsSession, dh_to_pylist: Callable[[Table], list[dict[str, Any]]]
) -> list[dict[str, Any]]:
    settle(ws_session)
    assert ws_session.session._core.diagnostics.barrier().wait(TIMEOUT)
    rows = dh_to_pylist(ws_session.session.connections())
    return [r for r in rows if r["Transport"] == "WebSocket"]


# --- Connections and streams, end to end ------------------------------------------------------


def test_the_channel_keys_are_declared_before_its_first_subscribe(ws_session: WsSession) -> None:
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.subscribed("T.AAPL")
    required = ("set_required", "T", frozenset({"Ticker", "SipTimestamp"}), "Ticker")
    assert ws_session.journal[:2] == [required, ("subscribe", ("T.AAPL",))]
    ws_session.session.release(result)


def test_a_stream_publishes_only_its_symbols(
    ws_session: WsSession,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    *,
    rows_when: RowsWhen,
) -> None:
    aapl = ws_session.query(TRADES, ["AAPL"]).append_only()
    every = ws_session.query(TRADES, dm.ALL).append_only()
    ws_session.subscribed("T.AAPL", "T.*")
    # A status beside the data is not a data item, so `send` does not wait for it.
    status: dict[str, object] = {"ev": "status", "status": "info", "message": "a notice"}
    ws_session.send(trade("AAPL", 1.5), status, trade("MSFT", 2.0))
    ws_session.flush()
    assert [r["Price"] for r in rows_when(aapl, lambda rows: len(rows) == 1)] == [1.5]
    every_rows = rows_when(every, lambda rows: len(rows) == 2)
    assert sorted(r["Symbol"] for r in every_rows) == ["AAPL", "MSFT"]
    rows = streams_of(ws_session, dh_to_pylist)
    assert [(r["Symbols"], r["RowsPublished"], r["Host"], r["Path"]) for r in rows] == [
        ("AAPL", 1, "socket.test", "/stocks"),
        ("ALL", 2, "socket.test", "/stocks"),
    ]


def test_equal_queries_share_one_stream_and_one_connection(ws_session: WsSession) -> None:
    first = ws_session.query(TRADES, ["AAPL"]).blink()
    second = ws_session.query(TRADES, "AAPL").last_by()
    quotes = ws_session.query(QUOTES, ["AAPL"]).blink()
    ws_session.subscribed("T.AAPL", "Q.AAPL")
    assert len(ws_session.urls) == 1
    assert ws_session.decisions == [(AssetClass.STOCKS, ChannelFamily.TICK)] * 2
    assert ws_session.connection().leases == 2
    for result in (first, second, quotes):
        ws_session.session.release(result)


def test_wire_symbols_become_canonical(
    ws_session: WsSession,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    *,
    rows_when: RowsWhen,
) -> None:
    result = ws_session.query(FOREX, ["C:EURUSD"]).append_only()
    ws_session.subscribed("C.EUR-USD")
    ws_session.send({"ev": "C", "p": "EUR/USD", "a": 1.1, "b": 1.0, "t": 1_700_000_000_000, "x": 1})
    ws_session.connection("wss://socket.test/forex").flusher.flush()
    shown = rows_when(result, lambda rows: len(rows) == 1)
    assert [r["Symbol"] for r in shown] == ["C:EURUSD"]


def test_an_fmv_frame_in_the_canonical_spelling_reaches_its_forex_stream(
    ws_session: WsSession, *, rows_when: RowsWhen
) -> None:
    # FMV frames name the pair `C:EURUSD`, while the topic spells it `EUR-USD`.
    result = ws_session.query(FOREX_FMV, ["C:EURUSD"]).append_only()
    ws_session.subscribed("FMV.EUR-USD")
    ws_session.send({"ev": "FMV", "sym": "C:EURUSD", "fmv": 1.1, "t": 1_700_000_000_000_000_000})
    ws_session.connection(ws_session.urls[-1]).flusher.flush()
    shown = rows_when(result, lambda rows: len(rows) == 1)
    assert [r["Symbol"] for r in shown] == ["C:EURUSD"]


def test_a_refused_topic_fails_only_its_streams(
    ws_session: WsSession,
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    *,
    rows_when: RowsWhen,
) -> None:
    fake_ws_server.refuse["Q.AAPL"] = "not authorized to access Q.AAPL"
    trades = ws_session.query(TRADES, ["AAPL"]).append_only()
    ws_session.subscribed("T.AAPL")
    quotes = ws_session.query(QUOTES, ["AAPL"]).blink()
    ws_session.wait(lambda s: any(status.kind is StatusKind.REFUSED for status in s.statuses))
    ws_session.flush()
    rows = streams_of(ws_session, dh_to_pylist)
    assert [r["State"] for r in rows] == ["RUNNING", "FAILED"]
    assert (
        "socket.test/stocks refused the subscription to Q.AAPL; Massive said: not authorized "
        "to access Q.AAPL"
    ) in rows[1]["Error"]
    assert "MassiveConfig.ws_hosts[AssetClass.STOCKS]" in rows[1]["Error"]
    ws_session.send(trade("AAPL"))
    ws_session.flush()
    rows_when(trades, lambda rows: len(rows) == 1)  # the connection kept running
    del quotes


def test_publish_waits_for_every_channel_of_a_frame(
    ws_session: WsSession, monkeypatch: pytest.MonkeyPatch, *, rows_when: RowsWhen
) -> None:
    # The client delivers a frame one channel at a time; the quote, delivered second and
    # slowly, must still reach the flusher before `publish` flushes, or with an hourly flush
    # interval its row would never be published.
    trades = ws_session.query(TRADES, ["AAPL"]).append_only()
    quotes = ws_session.query(QUOTES, ["AAPL"]).append_only()
    ws_session.subscribed("T.AAPL", "Q.AAPL")
    connection = ws_session.connection()
    deliver = connection._on_data
    pause = threading.Event()  # never set; only slows the second channel

    def slow_quotes(
        channel: str,
        rows: list[dict[str, object]],
        *,
        rejects: list[_massive.Reject],
        drift: _massive.DriftReport,
    ) -> None:
        if channel == "Q":
            pause.wait(0.3)
        deliver(channel, rows, rejects=rejects, drift=drift)

    monkeypatch.setattr(connection, "_on_data", slow_quotes)
    quote = {"ev": "Q", "sym": "AAPL", "bx": 4, "bp": 1.0, "bs": 100, "ax": 7, "ap": 1.1}
    ws_session.publish(trade("AAPL"), quote | {"as": 100, "t": 1_700_000_000_000, "z": 3})
    rows_when(trades, lambda rows: len(rows) == 1)
    rows_when(quotes, lambda rows: len(rows) == 1)


DROP_LOGGER = "deephaven_massive._tables.diagnostics"


def drop_warnings(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [
        r.getMessage()
        for r in caplog.records
        if r.name == DROP_LOGGER and r.levelno == logging.WARNING and "dropped" in r.getMessage()
    ]


def test_a_row_without_its_time_is_dropped_and_fails_no_stream(
    ws_session: WsSession,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    caplog: pytest.LogCaptureFixture,
    *,
    rows_when: RowsWhen,
) -> None:
    aapl = ws_session.query(TRADES, ["AAPL"]).append_only()
    msft = ws_session.query(TRADES, ["MSFT"]).blink()
    every = ws_session.query(TRADES, dm.ALL).append_only()
    ws_session.subscribed("T.AAPL", "T.MSFT", "T.*")
    bad = trade("MSFT")
    del bad["t"]  # a required key
    with caplog.at_level(logging.WARNING, logger=DROP_LOGGER):
        ws_session.send(bad, trade("AAPL"))
        ws_session.flush()
        rows_when(aapl, lambda rows: len(rows) == 1)
        rows_when(every, lambda rows: len(rows) == 1)
    rows = streams_of(ws_session, dh_to_pylist)
    assert [r["State"] for r in rows] == ["RUNNING"] * 3
    [warning] = drop_warnings(caplog)
    assert warning.startswith("test.trades: dropped 1 WS row(s) whose key cannot be read")
    assert "/stocks channel T: dropped a T row for MSFT from 127.0.0.1/stocks: no SipTimestamp" in (
        warning
    )
    del msft


def test_rows_without_a_symbol_are_dropped_and_the_all_stream_keeps_running(
    ws_session: WsSession,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    caplog: pytest.LogCaptureFixture,
    *,
    rows_when: RowsWhen,
) -> None:
    aapl = ws_session.query(TRADES, ["AAPL"]).blink()
    every = ws_session.query(TRADES, dm.ALL).append_only()
    ws_session.subscribed("T.AAPL", "T.*")
    nameless = trade("AAPL")
    del nameless["sym"]
    unreadable = trade("not a symbol")
    del unreadable["t"]
    with caplog.at_level(logging.WARNING, logger=DROP_LOGGER):
        ws_session.send(nameless, unreadable, trade("MSFT"))
        ws_session.flush()
        shown = rows_when(every, lambda rows: len(rows) == 1)
    assert [r["Symbol"] for r in shown] == ["MSFT"]
    rows = streams_of(ws_session, dh_to_pylist)
    assert [r["State"] for r in rows] == ["RUNNING", "RUNNING"]
    # One WARNING for the dataset, counting both rows and naming the first one's reason.
    [warning] = drop_warnings(caplog)
    assert warning.startswith("test.trades: dropped 2 WS row(s) whose key cannot be read")
    assert warning.endswith(
        "/stocks channel T: dropped a T row from 127.0.0.1/stocks: no Ticker (wire key 'sym'), "
        "which "
        "the dataset needs; such rows are dropped and counted, and the running count is logged "
        "at DEBUG"
    )
    del aapl


def test_a_timestamp_out_of_bounds_drops_its_row_and_fails_no_stream(
    ws_session: WsSession,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    *,
    rows_when: RowsWhen,
    caplog: pytest.LogCaptureFixture,
) -> None:
    aapl = ws_session.query(TRADES, ["AAPL"]).append_only()
    msft = ws_session.query(TRADES, ["MSFT"]).blink()
    ws_session.subscribed("T.AAPL", "T.MSFT")
    with caplog.at_level(logging.WARNING, logger="deephaven_massive"):
        ws_session.send(trade("MSFT", t=5), trade("AAPL"))
        ws_session.flush()
        rows_when(aapl, lambda rows: len(rows) == 1)
    rows = streams_of(ws_session, dh_to_pylist)
    assert [r["State"] for r in rows] == ["RUNNING", "RUNNING"]
    [warning] = drop_warnings(caplog)
    assert "timestamp unit mismatch for test.trades.Timestamp: 5 ms" in warning
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]
    del msft


def test_drift_is_logged_once_and_shown(
    ws_session: WsSession,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.subscribed("T.AAPL")
    with caplog.at_level(logging.WARNING):
        ws_session.send(trade("AAPL", zz=1))
        ws_session.send(trade("AAPL", zz=2))
        ws_session.flush()
    assert caplog.text.count("field 'zz' is not in the schema") == 1
    [connection] = connections_of(ws_session, dh_to_pylist)
    assert connection["SchemaDrift"] == "T:zz+"
    [stream] = streams_of(ws_session, dh_to_pylist)
    assert stream["SchemaDrift"] == "WebSocket:zz+"
    ws_session.session.release(result)


def test_releasing_the_last_result_unsubscribes(ws_session: WsSession) -> None:
    first = ws_session.query(TRADES, ["AAPL"]).blink()
    second = ws_session.query(TRADES, ["AAPL"]).ring(5)
    ws_session.subscribed("T.AAPL")
    ws_session.session.release(first)
    ws_session.session.release(second)
    ws_session.unsubscribed("T.AAPL")
    assert ws_session.connection().leases == 0


def test_a_derived_table_keeps_the_stream_alive(
    ws_session: WsSession, fake_ws_server: FakeMassiveServer, rows_when: RowsWhen
) -> None:
    result = ws_session.query(TRADES, ["AAPL"]).append_only()
    ws_session.subscribed("T.AAPL")
    scope = new_scope()  # the user's own scope, released below
    with scope.open():
        derived = [result.tail(100)]
    ws_session.session.release(result)
    del result
    gc.collect()
    ws_session.send(trade("AAPL"))
    ws_session.flush()
    rows_when(derived[0], lambda rows: len(rows) == 1)
    assert not any(json.loads(f)["action"] == "unsubscribe" for f in fake_ws_server.frames())
    derived.clear()
    scope.release()
    ws_session.unsubscribed("T.AAPL")


def test_a_reactive_table_follows_its_rows(
    ws_session: WsSession,
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    *,
    rows_when: RowsWhen,
) -> None:
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    watchlist.add(new_table([string_col("Symbol", ["AAPL"])]))
    result = ws_session.query(TRADES, watchlist).append_only()
    ws_session.subscribed("T.AAPL")
    ws_session.send(trade("AAPL"), trade("MSFT"))
    ws_session.flush()
    rows_when(result, lambda rows: len(rows) == 1)
    watchlist.add(new_table([string_col("Symbol", ["MSFT"])]))
    ws_session.subscribed("T.MSFT")
    watchlist.delete(new_table([string_col("Symbol", ["AAPL"])]))
    ws_session.unsubscribed("T.AAPL")
    ws_session.send(trade("AAPL", 2.0), trade("MSFT", 3.0))
    ws_session.flush()
    shown = rows_when(result, lambda rows: len(rows) == 2)
    assert [r["Symbol"] for r in shown] == ["AAPL", "MSFT"]
    sent = [json.loads(f)["params"] for f in fake_ws_server.frames()]
    assert sent == ["T.AAPL", "T.MSFT", "T.AAPL"]


def failed_after_flush(ws_session: WsSession, table: Table) -> bool:
    # Flush the connections, so a failure the flush thread was handed is applied.
    ws_session.flush()
    return bool(table.j_table.isFailed())


def test_a_late_watchlist_row_that_is_not_canonical_fails_the_stream_under_raise(
    make_ws_session: Callable[..., WsSession],
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    ws_session = make_ws_session(noncanonical_symbols=dm.OnError.RAISE)
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    watchlist.add(new_table([string_col("Symbol", ["AAPL"])]))
    result = ws_session.query(TRADES, watchlist).append_only()
    ws_session.subscribed("T.AAPL")
    watchlist.add(new_table([string_col("Symbol", ["msft"])]))
    wait_for(lambda: failed_after_flush(ws_session, result))
    (row,) = streams_of(ws_session, dh_to_pylist)
    assert row["State"] == "FAILED"
    assert row["Error"].startswith("test.trades: a symbol table holds 'msft'")
    assert "dm.canonicalize_symbols(table, dm.AssetClass.STOCKS, " in row["Error"]
    # The row that is not canonical never became a topic.
    sent = {json.loads(f)["params"] for f in fake_ws_server.frames()}
    assert sent == {"T.AAPL"}


def test_a_watchlist_row_that_is_not_canonical_is_skipped_and_shown(
    ws_session: WsSession,
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    watchlist.add(new_table([string_col("Symbol", ["AAPL", "msft"])]))
    result = ws_session.query(TRADES, watchlist).append_only()
    ws_session.subscribed("T.AAPL")
    skipped = (
        "skipping 1 symbol-table value this dataset does not take: 'msft'; fix or delete that "
        f"row, or report it at {ISSUES_URL} if Massive itself writes it"
    )

    def error() -> object:
        ws_session.flush()
        (row,) = streams_of(ws_session, dh_to_pylist)
        return row["Error"]

    wait_for(lambda: error() == skipped)
    watchlist.delete(new_table([string_col("Symbol", ["msft"])]))  # fixed: the text clears
    watchlist.add(new_table([string_col("Symbol", ["MSFT"])]))
    ws_session.subscribed("T.MSFT")
    wait_for(lambda: error() is None)
    (row,) = streams_of(ws_session, dh_to_pylist)
    assert row["State"] != "FAILED"
    sent = {json.loads(f)["params"] for f in fake_ws_server.frames()}
    assert "T.msft" not in ",".join(sent)  # the bad row never became a topic
    failed = bool(result.j_table.isFailed())  # pytest cannot describe a Java method
    assert not failed


def test_a_frame_symbol_the_library_cannot_read_fails_the_all_streams_under_raise(
    make_ws_session: Callable[..., WsSession], dh_to_pylist: Callable[[Table], list[dict[str, Any]]]
) -> None:
    ws_session = make_ws_session(unreadable_symbols=dm.OnError.RAISE)
    every = ws_session.query(TRADES, dm.ALL).append_only()
    ws_session.subscribed("T.*")
    ws_session.send(trade("BRK/B"))
    wait_for(lambda: failed_after_flush(ws_session, every))
    (row,) = streams_of(ws_session, dh_to_pylist)
    assert row["State"] == "FAILED"
    assert row["Error"].startswith("test.trades: Massive sent the symbol 'BRK/B', a spelling")


def test_an_unreadable_frame_symbol_fails_the_symbol_streams_too_under_raise(
    make_ws_session: Callable[..., WsSession], dh_to_pylist: Callable[[Table], list[dict[str, Any]]]
) -> None:
    ws_session = make_ws_session(unreadable_symbols=dm.OnError.RAISE)
    # The row cannot be routed, so no symbol stream may starve on it silently.
    one = ws_session.query(TRADES, ["AAPL"]).append_only()
    ws_session.subscribed("T.AAPL")
    ws_session.send(trade("BRK/B"))
    wait_for(lambda: failed_after_flush(ws_session, one))
    (row,) = streams_of(ws_session, dh_to_pylist)
    assert row["State"] == "FAILED"
    assert row["Error"].startswith("test.trades: Massive sent the symbol 'BRK/B', a spelling")


def test_a_failing_upstream_watchlist_fails_the_stream(
    ws_session: WsSession,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    # A canonicalize_symbols table under RAISE fails on a bad row; the stream must fail with
    # it rather than keep its old topics, and the failure is one ERROR, the stream's.
    source = input_table({"Symbol": dtypes.string})
    source.add(new_table([string_col("Symbol", ["AAPL"])]))
    watchlist = dm.canonicalize_symbols(source, AssetClass.STOCKS)
    result = ws_session.query(TRADES, watchlist).append_only()
    ws_session.subscribed("T.AAPL")
    with caplog.at_level(logging.DEBUG, logger="deephaven_massive"):
        source.add(new_table([string_col("Symbol", ["BRK-B"])]))
        wait_for(lambda: failed_after_flush(ws_session, result))
        (row,) = streams_of(ws_session, dh_to_pylist)
    assert row["State"] == "FAILED"
    assert row["Error"].startswith("the symbol table passed to test.trades failed upstream")
    errors = [
        r.getMessage()
        for r in caplog.records
        if r.levelno >= logging.ERROR and "failed upstream" in r.getMessage()
    ]
    assert len(errors) == 1
    assert errors[0].startswith("test.trades stream 1 failed: the symbol table passed to")


def stream_blink(ws_session: WsSession) -> Table:
    # The blink table of the session's only stream, which no result exposes.
    (state,) = ws_session.session._core._streams.values()
    assert state.handle is not None
    return state.handle.blink


def cycle_ended(ws_session: WsSession) -> None:
    # Taking the update graph's shared lock waits for the running update cycle to end, so every
    # notification of the cycle that delivered a failure has run, and survived, by now.
    with update_graph.shared_lock(ws_session.session._core.exec_ctx.update_graph):
        pass


@pytest.mark.parametrize("keys", [(), ("Exchange",)], ids=["by symbol", "by exchange"])
def test_a_failing_watchlist_fails_a_last_by_stream_once(
    ws_session: WsSession,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    keys: tuple[str, ...],
) -> None:
    # The watchlist fails the result first, and its stream's failure follows a few cycles
    # later: a result that `where_in` filtered by the watchlist would get that second failure,
    # and Deephaven ends the process on it.
    source = input_table({"Symbol": dtypes.string})
    source.add(new_table([string_col("Symbol", ["AAPL"])]))
    watchlist = dm.canonicalize_symbols(source, AssetClass.STOCKS)
    result = ws_session.query(TRADES, watchlist).last_by(*keys)
    ws_session.subscribed("T.AAPL")
    blink = stream_blink(ws_session)
    source.add(new_table([string_col("Symbol", ["BRK-B"])]))
    wait_for(lambda: failed_after_flush(ws_session, blink))
    cycle_ended(ws_session)
    assert result.j_table.isFailed()
    (row,) = streams_of(ws_session, dh_to_pylist)
    assert row["State"] == "FAILED"
    assert row["Error"].startswith("the symbol table passed to test.trades failed upstream")


def watching_streams(ws_session: WsSession, rows_when: RowsWhen) -> tuple[Table, Table]:
    # Quotes of a watchlist, and trades of the symbols those quotes hold, both by symbol.
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    watchlist.add(new_table([string_col("Symbol", ["AAPL"])]))
    quotes = ws_session.query(QUOTES, watchlist).last_by()
    trades = ws_session.query(TRADES, quotes.view("Symbol")).last_by()
    ws_session.subscribed("Q.AAPL")
    quote = {"ev": "Q", "sym": "AAPL", "bx": 4, "bp": 1.0, "bs": 100, "ax": 7, "ap": 1.1}
    ws_session.publish(quote | {"as": 100, "t": 1_700_000_000_000, "z": 3})
    rows_when(quotes, lambda rows: len(rows) == 1)
    ws_session.subscribed("T.AAPL")  # the trades follow the quotes' symbols
    return quotes, trades


def test_closing_a_last_by_stream_that_watches_another_stream_fails_each_once(
    ws_session: WsSession, rows_when: RowsWhen
) -> None:
    # The close fails both streams in one pass; the downstream result is filtered by the
    # upstream result, so it hears of both failures, and must take only the first.
    quotes, trades = watching_streams(ws_session, rows_when)
    ws_session.session.close()
    wait_for(lambda: bool(quotes.j_table.isFailed() and trades.j_table.isFailed()))
    cycle_ended(ws_session)


def test_releasing_a_stream_another_stream_watches_keeps_the_watcher_following(
    ws_session: WsSession, rows_when: RowsWhen
) -> None:
    # Releasing or collecting the upstream result fails nothing: the downstream result's
    # watchlist keeps the upstream stream running, and the close then fails both once.
    quotes, trades = watching_streams(ws_session, rows_when)
    ws_session.session.release(quotes)
    del quotes
    gc.collect()
    ws_session.send(trade("AAPL"))
    ws_session.flush()
    rows_when(trades, lambda rows: [r["Symbol"] for r in rows] == ["AAPL"])
    ws_session.session.close()
    wait_for(lambda: bool(trades.j_table.isFailed()))
    cycle_ended(ws_session)


def test_a_frame_symbol_the_library_cannot_read_is_kept_by_default(
    make_ws_session: Callable[..., WsSession], *, rows_when: RowsWhen
) -> None:
    warned = make_ws_session()
    every = warned.query(TRADES, dm.ALL).append_only()
    warned.subscribed("T.*")
    warned.send(trade("BRK/B"), trade("AAPL"))
    warned.flush()
    shown = rows_when(every, lambda rows: len(rows) == 2)
    assert [(r["Symbol"], r["RawSymbol"]) for r in shown] == [(None, "BRK/B"), ("AAPL", None)]


def test_a_derived_table_keeps_following_the_watchlist(
    ws_session: WsSession, rows_when: RowsWhen
) -> None:
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    watchlist.add(new_table([string_col("Symbol", ["AAPL"])]))
    result = ws_session.query(TRADES, watchlist).append_only()
    ws_session.subscribed("T.AAPL")
    derived = result.tail(100)
    ws_session.session.release(result)
    del result
    gc.collect()
    watchlist.add(new_table([string_col("Symbol", ["MSFT"])]))
    ws_session.subscribed("T.MSFT")
    ws_session.send(trade("MSFT"))
    ws_session.flush()
    rows_when(derived, lambda rows: len(rows) == 1)


def test_static_options_beyond_the_cap_raise(
    ws_session: WsSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(ws, "OPTIONS_CAP", 2)
    monkeypatch.setattr(ws_connection, "OPTIONS_CAP", 2)
    contracts = ["O:SPY251219C00600000", "O:SPY251219C00610000", "O:SPY251219C00620000"]
    with pytest.raises(ValueError, match="3 new option topics and 0 in use exceed the 2"):
        ws_session.query(OPTIONS, contracts).blink()
    first = ws_session.query(OPTIONS, contracts[:2]).blink()
    with pytest.raises(ValueError, match="1 new option topics and 2 in use"):
        ws_session.query(OPTIONS, contracts[2:]).blink()
    every = ws_session.query(OPTIONS, dm.ALL).blink()  # a wildcard counts zero
    ws_session.session.release(first)
    ws_session.session.release(every)


def test_the_cap_message_counts_topics_not_contracts(
    ws_session: WsSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Quotes of the contracts already traded are new topics, though no contract is new.
    monkeypatch.setattr(ws, "OPTIONS_CAP", 3)
    monkeypatch.setattr(ws_connection, "OPTIONS_CAP", 3)
    contracts = ["O:SPY251219C00600000", "O:SPY251219C00610000"]
    trades = ws_session.query(OPTIONS, contracts).blink()
    with pytest.raises(
        ValueError,
        match=r"^test.option_quotes: 2 new option topics and 2 in use exceed the 3 topics one "
        r"/options connection carries; a topic is one contract on one channel",
    ):
        ws_session.query(OPTION_QUOTES, contracts).blink()
    ws_session.session.release(trades)


def test_reactive_options_beyond_the_cap_are_deferred(
    ws_session: WsSession,
    monkeypatch: pytest.MonkeyPatch,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    monkeypatch.setattr(ws, "OPTIONS_CAP", 1)
    monkeypatch.setattr(ws_connection, "OPTIONS_CAP", 1)
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    contracts = ["O:SPY251219C00610000", "O:SPY251219C00600000"]
    watchlist.add(new_table([string_col("Symbol", contracts)]))
    result = ws_session.query(OPTIONS, watchlist).blink()
    ws_session.subscribed("T.O:SPY251219C00600000")
    connection = ws_session.connection("wss://socket.test/options")
    ws_session.on_loop(connection, lambda: None)
    [row] = streams_of(ws_session, dh_to_pylist)
    assert (row["State"], row["Deferred"], row["Error"]) == (
        "RUNNING",
        1,
        "over capacity: 2/1; 1 deferred",
    )
    watchlist.delete(new_table([string_col("Symbol", ["O:SPY251219C00600000"])]))
    ws_session.subscribed("T.O:SPY251219C00610000")
    ws_session.on_loop(connection, lambda: None)
    [row] = streams_of(ws_session, dh_to_pylist)
    assert (row["Deferred"], row["Error"]) == (0, None)
    del result


def unknown_futures(*_: object) -> HostDecision:
    return HostDecision("socket.test", Recency.UNKNOWN, "no timeframe in futures snapshot")


def test_a_failed_open_returns_its_lease(
    ws_session: WsSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    def broken(*_: object, **__: object) -> object:
        raise RuntimeError("no tables")

    monkeypatch.setattr(ws, "open_stream_tables", broken)
    with pytest.raises(RuntimeError, match="no tables"):
        ws_session.query(TRADES, ["AAPL"]).blink()
    assert ws_session.connection().leases == 0
    assert ws_session.connections.group.streams == {}


def test_a_watch_that_cannot_start_releases_the_stream(
    ws_session: WsSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    def broken(self: object) -> None:
        raise RuntimeError("no watch")

    monkeypatch.setattr(SymbolWatch, "start", broken)
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    with pytest.raises(RuntimeError, match="no watch"):
        ws_session.query(TRADES, watchlist).blink()
    assert ws_session.connection().leases == 0


def test_a_closed_transport_opens_nothing(ws_session: WsSession) -> None:
    request = ws_session.query(TRADES, ["AAPL"])._request
    ws_session.connections.close()
    with pytest.raises(dm.MassiveError, match="is closed"):
        ws_session.connections.open_stream(
            ws_session.session._core, TRADES, request, channel="T", stream_id=99
        )


def test_a_shutdown_after_the_connection_ended_is_logged(
    ws_session: WsSession, caplog: pytest.LogCaptureFixture
) -> None:
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    connection = ws_session.connection()
    connection.close()
    assert connection.join(TIMEOUT)
    with (
        caplog.at_level(logging.DEBUG, logger=ws.__name__),
        caplog.at_level(logging.DEBUG, logger=ws_connection.__name__),
    ):
        ws_session.session.release(result)
        wait_for(lambda: "has closed; dropped release_stream" in caplog.text)
    [dropped] = [r for r in caplog.records if "dropped release_stream" in r.getMessage()]
    assert dropped.levelno == logging.DEBUG  # an ended connection is expected, not news


def test_a_failing_shutdown_is_logged(
    ws_session: WsSession, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    def broken(stream_id: int) -> None:
        raise RuntimeError(f"registry {stream_id}")

    monkeypatch.setattr(ws_session.session._core, "stream_shut_down", broken)
    ws_session.connections._shut_down(42)
    assert "the shutdown of stream 42 failed" in caplog.text


def test_the_flush_thread_publishes_by_itself(
    make_ws_session: Callable[..., WsSession], rows_when: RowsWhen
) -> None:
    custom_session = make_ws_session(flush_interval=0.01)
    result = custom_session.query(TRADES, ["AAPL"]).append_only()
    custom_session.subscribed("T.AAPL")
    custom_session.send(trade("AAPL"))
    rows_when(result, lambda rows: len(rows) == 1)


def spare_connection(ws_session: WsSession, host: str) -> ws_connection.Connection:
    return ws_connection.Connection(
        ws_session.connections.group,
        exec_ctx=ws_session.session._core.exec_ctx,
        url=f"wss://{host}/stocks",
        asset_class=AssetClass.STOCKS,
        family=ChannelFamily.TICK,
        decision=REALTIME,
        sessions=None,
    )


def test_a_delayed_start_waits_and_close_cancels_it(ws_session: WsSession) -> None:
    cancelled = spare_connection(ws_session, "never.test")
    cancelled.start(delay=60.0)
    cancelled.start()  # a second start does nothing
    cancelled.close()
    cancelled.start()  # nor does a start after close
    assert cancelled.join(TIMEOUT)
    assert ws_session.opens == 0
    late = spare_connection(ws_session, "late.test")
    late.start(delay=0.01)
    ws_session.wait(lambda s: s.opens == 1)
    late.close()
    assert late.join(TIMEOUT)


def test_stress_concurrent_verbs_and_releases(ws_session: WsSession) -> None:
    # 16 threads open and release overlapping streams at once; every topic is released.
    symbols = [f"S{i}" for i in range(8)]

    def churn(worker: int) -> None:
        for round_ in range(10):
            symbol = symbols[(worker + round_) % len(symbols)]
            result = ws_session.query(TRADES, [symbol]).blink()
            ws_session.session.release(result)

    with ThreadPoolExecutor(16) as pool:
        list(pool.map(churn, range(16)))
    core = ws_session.session._core
    wait_for(lambda: not ws_session.connections.group.streams)
    connection = ws_session.connection()
    assert ws_session.on_loop(connection, lambda: connection.subscriptions.wanted) is False
    assert connection.leases == 0
    wait_for(lambda: core._streams == {})


# --- Edge cases of the connection's callbacks -------------------------------------------------


def test_late_callbacks_of_an_unknown_stream_are_ignored(
    ws_session: WsSession, dh_to_pylist: Callable[[Table], list[dict[str, Any]]]
) -> None:
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    connection = ws_session.connection()
    ws_session.subscribed("T.AAPL")
    ws_session.on_loop(connection, lambda: connection.release_stream(999))
    ws_session.on_loop(connection, lambda: connection.change_symbols(999, frozenset(), frozenset()))
    ws_session.connections._symbols_changed(999, frozenset({"X"}), frozenset())
    ws_session.on_loop(connection, connection._idle_close)  # a stream is open: it stays
    stale = Status(kind=StatusKind.REFUSED, topics=("T.AAPL",), message="not authorized")
    ws_session.on_loop(connection, lambda: connection._on_status(999, stale))  # an older socket's
    assert ws_session.connection() is connection
    ws_session.flush()
    [row] = streams_of(ws_session, dh_to_pylist)
    assert row["State"] == "RUNNING"
    ws_session.session.release(result)


def test_a_rejected_row_nobody_receives_fails_nothing(
    ws_session: WsSession, dh_to_pylist: Callable[[Table], list[dict[str, Any]]]
) -> None:
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.subscribed("T.AAPL")
    bad = trade("MSFT")
    del bad["t"]
    ws_session.send(bad)
    ws_session.flush()
    [row] = streams_of(ws_session, dh_to_pylist)
    assert row["State"] == "RUNNING"
    ws_session.session.release(result)


def test_a_closed_sessions_view_shows_its_connection_closed(
    ws_session: WsSession, dh_to_pylist: Callable[[Table], list[dict[str, Any]]]
) -> None:
    # The session's close closes the connection only it used; the connection's last row,
    # `CLOSED`, still reaches the session's diagnostics before they are released.
    view = ws_session.session.connections()
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.subscribed("T.AAPL")
    ws_session.close()
    (row,) = [r for r in dh_to_pylist(view) if r["Transport"] == "WebSocket"]
    assert row["State"] == "CLOSED"
    del result, view


def test_a_close_holding_the_update_graph_lock_joins_nothing(
    ws_session: WsSession,
    monkeypatch: pytest.MonkeyPatch,
    console_command: Callable[[Callable[[], None]], None],
) -> None:
    # In a console command no live table updates while close() waits, so it joins neither the
    # connections nor the futures schedule worker there; those daemons end on their own. A
    # close without the lock still joins both, each for a bounded time.
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    connection = ws_session.connection()
    joined: list[float] = []
    detached: list[bool] = []
    join, detach = connection.join, ws_session.connections.group.detach

    def joining(timeout: float) -> bool:
        joined.append(timeout)
        return join(timeout)

    def detaching(owner: ws.WsConnections, *, wait: bool = True) -> None:
        detached.append(wait)
        detach(owner, wait=wait)

    monkeypatch.setattr(connection, "join", joining)
    monkeypatch.setattr(ws_session.connections.group, "detach", detaching)
    console_command(ws_session.connections.close)
    assert (joined, detached) == ([], [False])
    assert join(TIMEOUT)  # the connection's threads end on their own
    ws_session.session.release(result)


@pytest.mark.parametrize("last", ["CONNECTED", "FAILED"])
def test_a_console_close_shows_its_connections_closed(
    *,
    ws_session: WsSession,
    monkeypatch: pytest.MonkeyPatch,
    console_command: Callable[[Callable[[], None]], None],
    rows_when: RowsWhen,
    last: str,
) -> None:
    # Joining no connection, close() under the lock cannot count on the client's own last row,
    # CLOSED, reaching the session before its diagnostics are released: it writes that row
    # itself, from the last row the client wrote (a FAILED one keeps its state), for each
    # family whose row the session shows, and the session leaves the connection's users. Here
    # the client never even closes (a server that never answers), yet a held m.connections()
    # view shows the final state once the command is over.
    m = ws_session.session
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.subscribed("T.AAPL")
    connection = ws_session.connection()
    view = m.connections()

    def states(rows: list[dict[str, Any]]) -> list[tuple[str, str, int, int]]:
        return [
            (r["Family"], r["State"], r["Topics"], r["BufferedRows"])
            for r in rows
            if r["Transport"] == "WebSocket"
        ]

    rows_when(view, lambda rows: states(rows) == [("TICK", "CONNECTED", 1, 0)])
    # The client thread rewrites its rows whenever its state changes (a late acknowledgement,
    # a summary), which would replace the rows set below. It writes no more rows from here on,
    # and a write it had already begun has ended once a later call has run on its loop.
    monkeypatch.setattr(connection, "_write_row", lambda: None)
    drained = threading.Event()
    connection._client.loop.call_soon_threadsafe(drained.set)
    assert drained.wait(TIMEOUT)
    with connection._lock:
        (row,) = connection._rows.values()
        connection._rows[ChannelFamily.TICK] = dataclasses.replace(
            row, state=ConnectionState(last), buffered_rows=7
        )
        # A family the session does not show a row of: show_closed writes nothing for it.
        connection._rows[ChannelFamily.FMV] = dataclasses.replace(row, family=ChannelFamily.FMV)
    client = connection._client
    stop = client.close
    monkeypatch.setattr(client, "close", lambda: None)
    try:
        console_command(m.close)
        # A FAILED row is shown as the client last wrote it; a CLOSED one holds nothing.
        final = ("TICK", "FAILED", 1, 7) if last == "FAILED" else ("TICK", "CLOSED", 0, 0)
        rows_when(view, lambda rows: states(rows) == [final])
        assert ws_session.connections not in connection.users
    finally:  # even when a check fails, the client must not outlive the test
        stop()
        assert client.join(TIMEOUT)
    del result, view


def test_a_close_without_the_lock_joins_its_connections(
    ws_session: WsSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    connection = ws_session.connection()
    joined: list[float] = []
    join = connection.join

    def joining(timeout: float) -> bool:
        joined.append(timeout)
        return join(timeout)

    monkeypatch.setattr(connection, "join", joining)
    ws_session.connections.close()
    assert joined == [ws.JOIN_TIMEOUT]
    ws_session.session.release(result)


def test_contracts_of_opening_streams_count_against_the_cap(
    ws_session: WsSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(ws, "OPTIONS_CAP", 2)
    monkeypatch.setattr(ws_connection, "OPTIONS_CAP", 2)
    first = ws_session.query(OPTIONS, ["O:SPY251219C00600000"]).blink()
    connection = ws_session.connection("wss://socket.test/options")
    with ws_session.connections.group.lock:
        connection.cap_pending[99] = frozenset({"T.O:SPY251219C00610000"})  # still opening
        with pytest.raises(ValueError, match="1 new option topics and 2 in use"):
            ws.WsConnections._check_cap(OPTIONS, connection, frozenset({"T.O:SPY251219C00620000"}))
        del connection.cap_pending[99]
    ws_session.session.release(first)


def test_the_documented_trade_frame_is_published(
    ws_session: WsSession,
    load_fixture: Callable[[str, str], bytes],
    rows_when: RowsWhen,
) -> None:
    # The frame of Massive's WebSocket quickstart: no `q`, `ds`, or `trfi`, which read as null.
    result = ws_session.query(TRADES, ["MSFT"]).append_only()
    ws_session.subscribed("T.MSFT")
    ws_session.send(*json.loads(load_fixture("ingest/ws", "stocks_trades.json")))
    ws_session.flush()
    rows = rows_when(result, lambda rows: len(rows) == 2)
    assert [(r["Symbol"], r["Price"], r["Size"], r["Conditions"]) for r in rows] == [
        ("MSFT", 215.9721, 100, None),
        ("MSFT", 215.989, 1, [37]),
    ]
    assert rows[0]["Timestamp"].value == 1_611_082_428_813_000_000


# --- Races of the connection lifecycle -------------------------------------------------------


def test_a_stream_opening_as_its_connection_fails_fails_too(
    ws_session: WsSession,
    fake_ws_server: FakeMassiveServer,
    *,
    monkeypatch: pytest.MonkeyPatch,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    # The connection fails fatally after the second stream leased it, before it is attached.
    first = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.subscribed("T.AAPL")
    real: Callable[..., object] = open_stream_tables

    def failing_meanwhile(*args: object, **kwargs: object) -> object:
        fake_ws_server.auth_replies.append("auth_failed")
        fake_ws_server.close(1011)
        ws_session.wait(lambda s: WsState.FAILED in s.states)
        return real(*args, **kwargs)

    monkeypatch.setattr(ws, "open_stream_tables", failing_meanwhile)
    with pytest.raises(dm.MassiveError, match=r"stream 2 failed: .*rejected the API key"):
        ws_session.query(TRADES, ["MSFT"]).blink()
    wait_for(lambda: [r["State"] for r in streams_of(ws_session, dh_to_pylist)] == ["FAILED"] * 2)
    rows = streams_of(ws_session, dh_to_pylist)
    assert all("rejected the API key" in r["Error"] for r in rows)
    del first


def test_a_verb_never_sees_a_failed_table_before_its_failure(
    ws_session: WsSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The failure is recorded before the table fails, so a verb in between builds its shape.
    q = ws_session.query(TRADES, ["AAPL"])
    first = q.blink()
    ws_session.subscribed("T.AAPL")
    core = ws_session.session._core
    recording = threading.Event()
    proceed = threading.Event()
    real = core.fail_stream

    def gated(stream_id: int, error: str) -> None:
        recording.set()
        assert proceed.wait(TIMEOUT)
        real(stream_id, error)

    monkeypatch.setattr(core, "fail_stream", gated)
    flusher = ws_session.connection().flusher
    flusher.fail([sink.stream_id for sink in flusher.routes.sinks()], dm.MassiveError("failed"))
    flushing = threading.Thread(target=ws_session.flush)
    flushing.start()
    assert recording.wait(TIMEOUT)
    second = q.append_only()  # the table has not failed yet: no raw DHError
    proceed.set()
    flushing.join(TIMEOUT)
    ws_session.session.release(second)
    del first


def test_a_new_channel_on_an_open_connection_declares_its_keys_first(ws_session: WsSession) -> None:
    trades = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.subscribed("T.AAPL")
    quotes = ws_session.query(QUOTES, ["AAPL"]).blink()
    ws_session.subscribed("Q.AAPL")
    required = ("set_required", "Q", frozenset({"Ticker", "SipTimestamp"}), "Ticker")
    position = ws_session.journal.index(required)
    assert ws_session.journal[position + 1] == ("subscribe", ("Q.AAPL",))
    ws_session.session.release(trades)
    ws_session.session.release(quotes)


def test_a_reactive_streams_row_follows_its_watchlist(
    ws_session: WsSession, dh_to_pylist: Callable[[Table], list[dict[str, Any]]]
) -> None:
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    watchlist.add(new_table([string_col("Symbol", ["MSFT", "AAPL"])]))
    result = ws_session.query(TRADES, watchlist).blink()
    ws_session.subscribed("T.AAPL", "T.MSFT")
    connection = ws_session.connection()
    ws_session.on_loop(connection, lambda: None)
    [row] = streams_of(ws_session, dh_to_pylist)
    assert (row["SymbolCount"], row["Symbols"]) == (2, "AAPL, MSFT")
    del result


def test_contracts_a_wildcard_covers_still_count_against_the_cap(
    ws_session: WsSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The wildcard may go at any time: a static contract the verb accepts is never deferred.
    monkeypatch.setattr(ws, "OPTIONS_CAP", 2)
    monkeypatch.setattr(ws_connection, "OPTIONS_CAP", 2)
    every = ws_session.query(OPTIONS, dm.ALL).blink()
    ws_session.subscribed("T.*")
    ws_session.wait(lambda s: any(status.kind is StatusKind.ACK for status in s.statuses))
    first = ws_session.query(OPTIONS, ["O:SPY251219C00600000", "O:SPY251219C00610000"]).blink()
    connection = ws_session.connection("wss://socket.test/options")
    ws_session.on_loop(connection, lambda: None)
    with pytest.raises(ValueError, match="1 new option topics and 2 in use"):
        ws_session.query(OPTIONS, ["O:SPY251219C00620000"]).blink()
    assert connection.cap_pending == {}  # the opened stream counts through the admitted set
    ws_session.session.release(first)
    ws_session.session.release(every)


def test_static_contracts_take_the_room_before_a_watchlists(
    ws_session: WsSession,
    monkeypatch: pytest.MonkeyPatch,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    monkeypatch.setattr(ws, "OPTIONS_CAP", 2)
    monkeypatch.setattr(ws_connection, "OPTIONS_CAP", 2)
    static = ws_session.query(OPTIONS, ["O:SPY251219C00690000"]).blink()  # sorts last
    ws_session.subscribed("T.O:SPY251219C00690000")
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    contracts = ["O:SPY251219C00600000", "O:SPY251219C00610000"]
    watchlist.add(new_table([string_col("Symbol", contracts)]))
    reactive = ws_session.query(OPTIONS, watchlist).blink()
    ws_session.subscribed("T.O:SPY251219C00600000")
    connection = ws_session.connection("wss://socket.test/options")
    ws_session.on_loop(connection, lambda: None)
    rows = streams_of(ws_session, dh_to_pylist)
    assert [(r["StreamId"], r["Deferred"]) for r in rows] == [(1, 0), (2, 1)]
    # The static stream's contract is known as static, so it keeps its place before the cap.
    static_topics = ws_session.on_loop(connection, lambda: dict(connection.subscriptions._static))
    assert static_topics == {"T.O:SPY251219C00690000": 1}
    del static, reactive


# --- Sessions sharing connections, and how long a stream runs ----------------------------------


def join(ws_session: WsSession) -> dm.MassiveSession:
    # A second session with the same key and settings, sharing the wrapper's connections; the
    # wrapper's own session is closed by its fixture, this one by the test.
    session = dm.MassiveSession(ws_session.config)
    connections = ws.WsConnections(
        session._core,
        resolve_host=ws_session._resolve,
        record_verdict=ws_session._record_verdict,
        fetch_rows=ws_session._fetch_rows,
        crypto_pair=ws_session._crypto_pair,
        client_factory=ws_session.factory,
    )
    assert connections.group is ws_session.connections.group
    cast("Any", session._core._ports)._streams = connections
    session._core.on_close(connections.close)
    return session


def jvm_collect() -> None:
    # A full collection on both sides: Python's cycles, then the JVM's heap.
    gc.collect()
    for _ in range(3):
        _JAVA_SYSTEM.gc()


def test_a_stream_waiting_for_a_slot_says_so(
    *,
    fake_ws_server: FakeMassiveServer,
    make_ws_session: Callable[..., WsSession],
    monkeypatch: pytest.MonkeyPatch,
    dh_to_pylist: ToPylist,
) -> None:
    # Another connection with the key (another process, here the fake's reply) holds the slot.
    monkeypatch.setattr(ws, "OPTIONS_CAP", 1)
    monkeypatch.setattr(ws_connection, "OPTIONS_CAP", 1)
    ws_session = make_ws_session(backoff=1.0)
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    contracts = ["O:SPY251219C00600000", "O:SPY251219C00610000"]
    watchlist.add(new_table([string_col("Symbol", contracts)]))
    capped = ws_session.query(OPTIONS, watchlist).blink()
    ws_session.subscribed("T.O:SPY251219C00600000")
    cap = "over capacity: 2/1; 1 deferred"
    wait_for(lambda: streams_of(ws_session, dh_to_pylist)[0]["Error"] == cap)
    fake_ws_server.auth_replies.append("max_connections")
    fake_ws_server.close()  # the socket drops; the reconnect finds the slot taken
    ws_session.wait(lambda h: WsState.WAITING_SLOT in h.states)

    # The stall and the options cap are two parts of the Error: both show, the stall first.
    capped_stall = f"{STALL}; {re.escape(cap)}"

    def stalled(*patterns: str) -> bool:
        errors = [r["Error"] or "" for r in streams_of(ws_session, dh_to_pylist)]
        return len(errors) == len(patterns) and all(
            re.fullmatch(p, e) for p, e in zip(patterns, errors, strict=True)
        )

    wait_for(lambda: stalled(capped_stall))
    every = ws_session.query(OPTION_QUOTES, dm.ALL).blink()  # opens during the wait
    wait_for(lambda: stalled(capped_stall, STALL))
    ws_session.wait(lambda h: h.opens == 2)  # the slot is free again
    wait_for(lambda: [r["Error"] for r in streams_of(ws_session, dh_to_pylist)] == [cap, None])
    del capped, every


SKIPPED_SPY = (
    "skipping 1 symbol-table value this dataset does not take: 'spy'; fix or delete that "
    f"row, or report it at {ISSUES_URL} if Massive itself writes it"
)
"""The `Error` text of a stream whose options watchlist holds the bad row `spy`."""


def test_skipped_rows_and_a_stall_neither_hide_nor_clear_each_other(
    *,
    fake_ws_server: FakeMassiveServer,
    make_ws_session: Callable[..., WsSession],
    monkeypatch: pytest.MonkeyPatch,
    dh_to_pylist: ToPylist,
) -> None:
    monkeypatch.setattr(ws, "OPTIONS_CAP", 1)
    monkeypatch.setattr(ws_connection, "OPTIONS_CAP", 1)
    ws_session = make_ws_session(backoff=1.0)
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    contracts = ["O:SPY251219C00600000", "O:SPY251219C00610000", "spy"]
    watchlist.add(new_table([string_col("Symbol", contracts)]))
    capped = ws_session.query(OPTIONS, watchlist).blink()
    ws_session.subscribed("T.O:SPY251219C00600000")

    def error() -> object:
        return streams_of(ws_session, dh_to_pylist)[0]["Error"]

    cap = "over capacity: 2/1; 1 deferred"
    wait_for(lambda: error() == f"{cap}; {SKIPPED_SPY}")
    fake_ws_server.auth_replies.append("max_connections")
    fake_ws_server.close()  # the socket drops; the reconnect finds the slot taken
    ws_session.wait(lambda h: WsState.WAITING_SLOT in h.states)
    skipped = f"; {re.escape(SKIPPED_SPY)}"

    def stalled(suffix: str) -> bool:
        pattern = f"{STALL}; {re.escape(cap)}{suffix}"  # each source keeps its own part
        return re.fullmatch(pattern, cast("str", error() or "")) is not None

    wait_for(lambda: stalled(skipped))  # the stall keeps the skipped row
    watchlist.delete(new_table([string_col("Symbol", ["spy"])]))
    wait_for(lambda: stalled(""))  # fixing the row keeps the stall
    watchlist.add(new_table([string_col("Symbol", ["spy"])]))
    wait_for(lambda: stalled(skipped))
    ws_session.wait(lambda h: h.opens == 2)  # the slot is free again: the stall clears alone
    wait_for(lambda: error() == f"{cap}; {SKIPPED_SPY}")
    del capped


def test_skipped_rows_and_the_options_cap_neither_hide_nor_clear_each_other(
    ws_session: WsSession,
    monkeypatch: pytest.MonkeyPatch,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    monkeypatch.setattr(ws, "OPTIONS_CAP", 1)
    monkeypatch.setattr(ws_connection, "OPTIONS_CAP", 1)
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    contracts = ["O:SPY251219C00610000", "O:SPY251219C00600000", "spy"]
    watchlist.add(new_table([string_col("Symbol", contracts)]))
    result = ws_session.query(OPTIONS, watchlist).blink()
    ws_session.subscribed("T.O:SPY251219C00600000")
    connection = ws_session.connection("wss://socket.test/options")

    def error() -> object:
        ws_session.on_loop(connection, lambda: None)
        return streams_of(ws_session, dh_to_pylist)[0]["Error"]

    cap = "over capacity: 2/1; 1 deferred"
    wait_for(lambda: error() == f"{cap}; {SKIPPED_SPY}")
    watchlist.delete(new_table([string_col("Symbol", ["spy"])]))
    wait_for(lambda: error() == cap)  # fixing the row keeps the cap's warning
    watchlist.add(new_table([string_col("Symbol", ["spy"])]))
    wait_for(lambda: error() == f"{cap}; {SKIPPED_SPY}")
    watchlist.delete(new_table([string_col("Symbol", ["O:SPY251219C00600000"])]))
    ws_session.subscribed("T.O:SPY251219C00610000")
    wait_for(lambda: error() == SKIPPED_SPY)  # the cap frees up and keeps the skipped row
    del result


def freed(core: weakref.ref[SessionCore]) -> bool:
    # Bounded wait in Python, not in one C-level wait: a free-threaded build merges a reference
    # dropped on another thread into its creator's count only while the creator runs Python.
    pause = threading.Event()  # never set; only paces the loop
    for _ in range(int(TIMEOUT / 0.02)):
        if core() is None:
            return True
        pause.wait(0.02)
    return core() is None


def test_a_session_moved_by_a_verdict_is_freed_without_the_cyclic_collector(
    fake_ws_server: FakeMassiveServer,
) -> None:
    # The old connection keeps the moved stream routed for its final flush; neither that nor
    # anything else may keep the session once nothing uses it, cycles or not.
    harness: WsSession = importlib.import_module("tests._fakes.ws_session").WsSession(
        fake_ws_server, decide=unknown_futures
    )
    gc.collect()
    gc.disable()
    try:
        harness.connections.group.reconnect_backoff = 0.01
        fake_ws_server.step_refusals.append("Real-time data requires an upgrade")
        result = harness.query(FUTURES, ["ESZ5"]).blink()
        harness.wait(lambda s: s.opens == 2)
        harness.subscribed("T.ESZ5")
        successor = harness.connection("wss://delayed.test/futures")
        core = weakref.ref(harness.session._core)
        harness.session.release(result)
        del result, harness.session, harness.connections
        assert freed(core)
        assert successor.join(TIMEOUT)  # the session's closer closed it
    finally:
        gc.enable()


def test_a_closed_session_that_shared_a_connection_is_freed_once_its_table_is_dropped(
    ws_session: WsSession,
) -> None:
    # The closed session created the shared connection, which keeps serving the other one.
    other = join(ws_session)
    theirs = ws_session.query(TRADES, ["AAPL"], other).append_only()
    ws_session.subscribed("T.AAPL")
    mine = ws_session.query(TRADES, ["AAPL"]).append_only()
    core = weakref.ref(other._core)
    gc.collect()
    gc.disable()
    try:
        other.close()
        del other, theirs
        assert freed(core)
    finally:
        gc.enable()
    ws_session.session.release(mine)


def test_an_inline_sessions_derived_table_keeps_running(
    fake_ws_server: FakeMassiveServer, dh_exec_ctx: object, rows_when: RowsWhen
) -> None:
    # `dm.MassiveSession().stocks.trades("AAPL").append_only().where(...)`: the session and the
    # library's result are dropped at once, but the derived table uses the stream, so the
    # stream, its connection, and its session keep running through every collection.
    # Built directly, not by the fixture, which would close the session this test frees.
    harness: WsSession = importlib.import_module("tests._fakes.ws_session").WsSession(
        fake_ws_server
    )
    ws_session = harness
    del harness
    core = weakref.ref(ws_session.session._core)
    scope = new_scope()  # the user's own scope, released below
    with scope.open():
        derived = [ws_session.query(TRADES, ["AAPL"]).append_only().where("Price > 0")]
    ws_session.subscribed("T.AAPL")
    connection = ws_session.connection()
    del ws_session.session, ws_session.connections
    jvm_collect()
    assert core() is not None
    ws_session.send(trade("AAPL"))
    connection.flusher.flush()
    rows_when(derived[0], lambda rows: len(rows) == 1)
    # Releasing the derived table stops the stream, frees the session, and closes the
    # connection that nothing else uses.
    derived.clear()
    scope.release()
    # Wait in Python, not in join(): a free-threaded build merges a reference dropped on another
    # thread into its owner's count only while the owner runs Python code.
    wait_for(lambda: core() is None)
    assert connection.join(TIMEOUT)
    assert WsState.CLOSED in ws_session.states


def test_a_watchlist_edit_after_a_jvm_collection_still_resubscribes(ws_session: WsSession) -> None:
    # Deephaven holds a table's listeners only weakly: the stream must hold its own.
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    watchlist.add(new_table([string_col("Symbol", ["AAPL"])]))
    result = ws_session.query(TRADES, watchlist).append_only()
    ws_session.subscribed("T.AAPL")
    jvm_collect()
    watchlist.add(new_table([string_col("Symbol", ["MSFT"])]))
    ws_session.subscribed("T.MSFT")
    ws_session.session.release(result)


def test_stress_two_sessions_open_and_release_while_one_closes(ws_session: WsSession) -> None:
    other = join(ws_session)
    symbols = ["AAPL", "MSFT", "IBM", "GOOG"]
    started = threading.Barrier(17)
    closed_errors: list[str] = []

    def work(index: int) -> None:
        session = ws_session.session if index % 2 else other
        started.wait(TIMEOUT)
        for round_ in range(20):
            try:
                result = ws_session.query(TRADES, [symbols[(index + round_) % 4]], session).blink()
            except dm.MassiveError as error:
                assert session is other
                closed_errors.append(str(error))
                return
            session.release(result)

    with ThreadPoolExecutor(16) as pool:
        futures = [pool.submit(work, index) for index in range(16)]
        started.wait(TIMEOUT)
        other.close()
        for future in futures:
            future.result(TIMEOUT)
    # A verb that races the close finds the session closed, or its stream failed by the close.
    name = other._core.label
    assert all(f"{name} is closed" in t or f"{name} was closed" in t for t in closed_errors)
    core = ws_session.session._core
    drained = threading.Event()
    core.defer(drained.set)
    assert drained.wait(TIMEOUT)
    wait_for(lambda: not ws_session.connections.group.streams)
    with ws_session.connections.group.lock:
        assert all(c.leases == 0 for c in ws_session.connections.group.connections.values())


def test_a_java_held_callback_of_a_freed_session_does_nothing() -> None:
    # The JVM may run a publisher's shutdown callback after the session's connections are gone.
    class _Gone:
        pass

    gone = weakref.ref(_Gone())
    ws._weakly(cast("weakref.ref[ws.WsConnections]", gone), "_shut_down", 1)


def test_an_ended_connection_holds_no_stream(
    ws_session: WsSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The client's terminal state lets every stream go on the loop thread, and the flush
    # thread clears its routes when it ends; a call that reaches the connection after that is
    # ignored, so no stream (nor, through it, a session) is left on a connection that is gone.
    other = join(ws_session)
    mine = ws_session.query(TRADES, ["AAPL"]).blink()
    theirs = ws_session.query(TRADES, ["MSFT"], other).blink()
    ws_session.subscribed("T.AAPL", "T.MSFT")
    connection = ws_session.connection()
    stream = cast(
        "ws_connection.ConnectionStream",
        ws_session.on_loop(connection, lambda: connection._streams[1]),
    )
    # A freed session shows no row: an update for its stream is dropped.
    monkeypatch.setattr(stream.sink, "_core", lambda: None)
    ws_session.on_loop(
        connection, lambda: connection._update_streams({1: StreamUpdate(deferred=1)})
    )
    monkeypatch.undo()
    connection.close()
    assert connection.join(TIMEOUT)
    assert connection._streams == {}
    assert connection.flusher.routes.sinks() == ()
    # The loop has ended: what still reaches the connection (here called directly, as no
    # post can run) leaves nothing behind.
    connection.add_stream(stream)
    connection.adopt([stream])
    connection.check_idle()
    assert connection._streams == {}
    other.close()  # its stream's release finds the loop gone
    ws_session.session.release(mine)
    del theirs


# --- The futures schedule calendar --------------------------------------------------------------


def looked_up(ws_session: WsSession, *tickers: str) -> None:
    # Wait until the calendar has asked Massive's contracts for every ticker.
    def asked() -> bool:
        sent = {
            ticker
            for path, query in ws_session.fetched
            if path == "/futures/v1/contracts"
            for ticker in cast("tuple[str, ...]", query["ticker.any_of"])
        }
        return set(tickers) <= sent

    ws_session.wait(lambda _: asked())


def test_a_futures_stream_looks_up_its_contracts_as_it_opens(ws_session: WsSession) -> None:
    result = ws_session.query(FUTURES, ["ESZ5", "GCQ6-GCJ7"]).blink()
    looked_up(ws_session, "ESZ5")
    calendar = ws_session.connections.futures_calendar()
    assert calendar is ws_session.connections.futures_calendar()
    assert calendar.wait_idle(15.0)
    # A spread is not a contract: it is never looked up.
    sent = [q["ticker.any_of"] for path, q in ws_session.fetched if path == "/futures/v1/contracts"]
    assert sent == [("ESZ5",)]
    ws_session.session.release(result)


def test_a_futures_watchlist_looks_up_the_contracts_it_adds(ws_session: WsSession) -> None:
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    watchlist.add(new_table([string_col("Symbol", ["ESZ5"])]))
    result = ws_session.query(FUTURES, watchlist).blink()
    looked_up(ws_session, "ESZ5")
    watchlist.add(new_table([string_col("Symbol", ["NQZ5"])]))
    looked_up(ws_session, "NQZ5")
    ws_session.session.release(result)


def test_a_futures_watchlist_change_during_a_close_is_dropped(
    ws_session: WsSession, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    # The session closes after the change found the stream's connection: the change is dropped
    # quietly, never raised into the watchlist's listener, which would fail the stream.
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    watchlist.add(new_table([string_col("Symbol", ["ESZ5"])]))
    result = ws_session.query(FUTURES, watchlist).blink()
    looked_up(ws_session, "ESZ5")
    (stream_id,) = ws_session.connections.group.streams

    def closed() -> futures_calendar.FuturesCalendar:
        raise dm.MassiveError("MassiveSession #1 is closed")

    monkeypatch.setattr(ws_session.connections, "futures_calendar", closed)
    with caplog.at_level(logging.DEBUG, logger="deephaven_massive._ingest.ws"):
        ws_session.connections._symbols_changed(stream_id, frozenset({"NQZ5"}), frozenset())
    assert (
        f"stream {stream_id}: a watchlist change (1 added, 0 removed) was dropped; the session "
        "is closing (MassiveSession #1 is closed)"
    ) in caplog.text
    ws_session.session.release(result)


def lookups(session: WsSession) -> list[tuple[str, str]]:
    # Each crypto ticker the transport looked up, with the thread it ran on.
    return [
        cast("tuple[str, str]", call[1:]) for call in session.journal if call[0] == "crypto_pair"
    ]


# Each of these reads as two different pairs (DOT-USD or DO-TUSD); only Massive's list says which.
AMBIGUOUS = {
    "X:DOTUSD": ("DOT", "USD"),  # DOT-USD or DO-TUSD
    "X:SHIBUSD": ("SHIB", "USD"),  # SHIB-USD or SHI-BUSD
    "X:USDTUSD": ("USDT", "USD"),  # USDT-USD or USD-TUSD
    "X:BTCPYUSD": ("BTC", "PYUSD"),  # BTC-PYUSD or BTCPY-USD
}


def test_an_ambiguous_crypto_pair_subscribes_the_topic_massive_gives(ws_session: WsSession) -> None:
    # Massive's crypto ticker list splits every pair, and each ticker is looked up once.
    ws_session.pair_answers.update(AMBIGUOUS)
    ws_session.pair_answers["X:ETHWBTC"] = ("ETH", "WBTC")  # ends in BTC, quotes in WBTC
    result = ws_session.query(CRYPTO, [*AMBIGUOUS, "X:BTCUSD", "X:ETHWBTC"]).blink()
    ws_session.subscribed(
        "XT.DOT-USD", "XT.SHIB-USD", "XT.USDT-USD", "XT.BTC-PYUSD", "XT.BTC-USD", "XT.ETH-WBTC"
    )
    looked_up = sorted([*AMBIGUOUS, "X:BTCUSD", "X:ETHWBTC"])  # every pair: none is guessed
    assert sorted(symbol for symbol, _ in lookups(ws_session)) == looked_up
    again = ws_session.query(CRYPTO, ["X:DOTUSD"]).append_only()  # another stream: cached
    assert sorted(symbol for symbol, _ in lookups(ws_session)) == looked_up
    for table in (result, again):
        ws_session.session.release(table)


def test_a_failed_crypto_pair_lookup_refuses_the_stream(ws_session: WsSession) -> None:
    with pytest.raises(
        dm.MassiveError,
        match=r"^cannot stream 'X:DOTUSD': 'X:DOTUSD' cannot be split into its two currencies: "
        r"Massive's reference data does not list it$",
    ):
        ws_session.query(CRYPTO, ["X:DOTUSD"]).blink()
    assert ws_session.urls == []  # no connection opened, so no topic guessed


def test_a_watchlist_row_of_an_ambiguous_pair_is_looked_up_off_the_update_graph(
    ws_session: WsSession, dh_to_pylist: Callable[[Table], list[dict[str, Any]]]
) -> None:
    ws_session.pair_answers["X:DOTUSD"] = ("DOT", "USD")
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    watchlist.add(new_table([string_col("Symbol", ["X:SHIBUSD"])]))
    ws_session.pair_answers["X:SHIBUSD"] = ("SHIB", "USD")
    result = ws_session.query(CRYPTO, watchlist).append_only()
    ws_session.subscribed("XT.SHIB-USD")  # the current row, looked up before the stream started
    watchlist.add(new_table([string_col("Symbol", ["X:DOTUSD"])]))
    ws_session.subscribed("XT.DOT-USD")  # a later row, looked up on the lookup worker
    (_, caller), (_, worker) = lookups(ws_session)
    assert caller == threading.current_thread().name
    assert worker.startswith(f"{ws_session.session._core.thread_prefix} crypto pairs")
    watchlist.add(new_table([string_col("Symbol", ["X:ETHTUSD"])]))  # Massive does not list it
    wait_for(lambda: failed_after_flush(ws_session, result))
    (row,) = streams_of(ws_session, dh_to_pylist)
    assert row["Error"].startswith("cannot stream 'X:ETHTUSD': 'X:ETHTUSD' cannot be split")
    assert "XT.DO-TUSD" not in {json.loads(f)["params"] for f in ws_session.server.frames()}


def test_a_lookup_that_finishes_after_its_stream_ended_changes_nothing(
    ws_session: WsSession,
) -> None:
    # A watchlist change waits on the lookup worker; if the stream has gone by the time the
    # answer arrives, the answer is kept for the session and nothing is posted or failed.
    ws_session.pair_answers["X:DOTUSD"] = ("DOT", "USD")
    ws_session.connections._change_pairs(999, frozenset({"X:DOTUSD"}), frozenset())
    assert [symbol for symbol, _ in lookups(ws_session)] == ["X:DOTUSD"]
    assert ws_session.connections.pairs() == {"X:DOTUSD": ("DOT", "USD")}
    assert ws_session.urls == []  # no connection was opened for it


def test_closing_the_transport_stops_the_lookup_worker(ws_session: WsSession) -> None:
    ws_session.connections.close()
    with pytest.raises(RuntimeError, match="after shutdown"):
        ws_session.connections._lookups.submit(print)


def test_a_watchlist_whose_first_row_massive_does_not_list_is_refused(
    ws_session: WsSession,
) -> None:
    watchlist = new_table([string_col("Symbol", ["X:ETHTUSD"])])
    with pytest.raises(dm.MassiveError, match=r"^cannot stream 'X:ETHTUSD': 'X:ETHTUSD' cannot"):
        ws_session.query(CRYPTO, watchlist).append_only()
    assert ws_session.urls == []  # no connection opened


def test_an_unexpected_lookup_failure_fails_the_stream(
    ws_session: WsSession,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    ws_session.pair_answers["X:DOTUSD"] = RuntimeError("the lookup broke")
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    watchlist.add(new_table([string_col("Symbol", ["X:BTCUSD"])]))
    result = ws_session.query(CRYPTO, watchlist).append_only()
    ws_session.subscribed("XT.BTC-USD")
    watchlist.add(new_table([string_col("Symbol", ["X:DOTUSD"])]))
    wait_for(lambda: failed_after_flush(ws_session, result))
    (row,) = streams_of(ws_session, dh_to_pylist)
    assert row["Error"] == (
        "looking up the crypto pairs ['X:DOTUSD'] failed: RuntimeError('the lookup broke'). "
        + LIBRARY_BUG
    )
    # A defect of the library: one ERROR, with its trace, for the lookup and the stream it fails.
    errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert [(r.getMessage(), r.exc_info is not None) for r in errors] == [
        (f"{ws_session.session._core.label}: {row['Error']}", True)
    ]


def test_a_watchlist_change_racing_close_is_dropped(ws_session: WsSession) -> None:
    # close() shuts the lookup worker down; a change that reaches it after that is dropped,
    # since the stream goes with the transport, and nothing is raised on the update graph.
    result = ws_session.query(CRYPTO, ["X:BTCUSD"]).append_only()
    ws_session.subscribed("XT.BTC-USD")
    (stream_id,) = ws_session.connections._mine
    ws_session.connections._lookups.shutdown()
    ws_session.connections._symbols_changed(stream_id, frozenset({"X:ETHUSD"}), frozenset())
    # The stream's own pair, before it started.
    assert [symbol for symbol, _ in lookups(ws_session)] == ["X:BTCUSD"]
    ws_session.session.release(result)


def test_every_crypto_pair_streams_without_a_lookup(ws_session: WsSession) -> None:
    result = ws_session.query(CRYPTO, dm.ALL).blink()
    ws_session.subscribed("XT.*")
    assert lookups(ws_session) == []
    ws_session.session.release(result)


def test_a_removal_never_overtakes_a_slow_lookup_of_the_same_pair(
    ws_session: WsSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The crypto pairs worker runs one change at a time, in order: a pair removed while its
    # addition still waits on the lookup is removed after it is added, never before, so the
    # stream does not end up subscribed to a pair its table no longer holds.
    gate, started = threading.Event(), threading.Event()
    lookup = ws_session.connections._crypto_pair
    change = ws_session.connections._change_pairs
    done: list[tuple[list[str], list[str]]] = []

    def slow(symbol: str) -> tuple[str, str]:
        if symbol == "X:DOTUSD":
            started.set()
            assert gate.wait(TIMEOUT)
        return lookup(symbol)

    def recorded(stream_id: int, added: frozenset[str], removed: frozenset[str]) -> None:
        change(stream_id, added, removed)
        with ws_session.cond:
            done.append((sorted(added), sorted(removed)))

    ws_session.connections._crypto_pair = slow
    # Records the order in which the worker finishes the changes.
    monkeypatch.setattr(ws_session.connections, "_change_pairs", recorded)
    ws_session.pair_answers["X:DOTUSD"] = ("DOT", "USD")
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    watchlist.add(new_table([string_col("Symbol", ["X:BTCUSD"])]))
    result = ws_session.query(CRYPTO, watchlist).append_only()
    ws_session.subscribed("XT.BTC-USD")
    watchlist.add(new_table([string_col("Symbol", ["X:DOTUSD"])]))
    assert started.wait(TIMEOUT)  # the addition waits on its lookup
    watchlist.delete(new_table([string_col("Symbol", ["X:DOTUSD"])]))
    watchlist.add(new_table([string_col("Symbol", ["X:ETHUSD"])]))
    gate.set()

    def position(entry: Callable[[tuple[list[str], list[str]]], bool]) -> int | None:
        with ws_session.cond:
            return next((i for i, e in enumerate(done) if entry(e)), None)

    def added_dot(e: tuple[list[str], list[str]]) -> bool:
        return "X:DOTUSD" in e[0]

    def removed_dot(e: tuple[list[str], list[str]]) -> bool:
        return "X:DOTUSD" in e[1]

    wait_for(lambda: position(added_dot) is not None and position(removed_dot) is not None)
    assert cast("int", position(added_dot)) < cast("int", position(removed_dot))
    ws_session.subscribed("XT.ETH-USD")
    ws_session.unsubscribed("XT.DOT-USD")
    ws_session.session.release(result)


def test_a_static_symbol_table_subscribes_its_symbols_once(
    ws_session: WsSession, dh_to_pylist: Callable[[Table], list[dict[str, Any]]]
) -> None:
    fixed = new_table([string_col("Symbol", ["AAPL", "MSFT"])])
    results = [ws_session.query(TRADES, fixed).blink(), ws_session.query(TRADES, fixed).last_by()]
    ws_session.subscribed("T.AAPL", "T.MSFT")
    [stream] = streams_of(ws_session, dh_to_pylist)
    assert (stream["SymbolCount"], stream["State"]) == (2, "RUNNING")
    for result in results:
        ws_session.session.release(result)


# --- logging -------------------------------------------------------------------------------------

_URL = "wss://socket.test/stocks"


def test_symbol_changes_and_shutdowns_are_logged(
    ws_session: WsSession, caplog: pytest.LogCaptureFixture
) -> None:
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    watchlist.add(new_table([string_col("Symbol", ["AAPL"])]))
    with (
        caplog.at_level(logging.DEBUG, logger=ws.__name__),
        caplog.at_level(logging.DEBUG, logger=ws_connection.__name__),
    ):
        result = ws_session.query(TRADES, watchlist).append_only()
        ws_session.subscribed("T.AAPL")
        watchlist.add(new_table([string_col("Symbol", ["MSFT"])]))
        ws_session.subscribed("T.MSFT")
        ws_session.session.release(result)
        ws_session.unsubscribed("T.AAPL")  # the stream shut down and released its topics
    assert "stream 1: its symbol table added ['MSFT'] and removed []" in caplog.messages
    assert (
        f"stream 1 shut down: its route is removed and 2 topics released on {_URL}"
        in caplog.messages
    )


# --- thread names --------------------------------------------------------------------------------

_DIVIDENDS_PATH = "/stocks/v1/dividends"
_DIVIDENDS = DatasetSpec(
    summary="A dataset for the tests.",
    name="stocks.dividends",
    asset_class=AssetClass.STOCKS,
    fetch_path=_DIVIDENDS_PATH,
    symbol_param="ticker.any_of",
    keys={(Transport.REST, _DIVIDENDS_PATH): SourceKeys(symbol=KeySpec("Ticker"))},
)


def test_every_library_thread_is_named_after_its_session_or_connection_group(
    fake_ws_server: FakeMassiveServer,
    make_ws_session: Callable[..., WsSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Every thread the library starts, recorded by name. The session's own (its writer and
    # reaper, a poller and its pool) carry the session's prefix; those of the connections,
    # which serve every session of their group (a client and its flush thread, a verdict's move
    # and delayed start, the futures schedule worker), carry the group's label.
    started: list[str] = []
    start = threading.Thread.start

    def recording(thread: threading.Thread) -> None:
        started.append(thread.name)
        start(thread)

    monkeypatch.setattr(threading.Thread, "start", recording)
    custom_session = make_ws_session(decide=unknown_futures)
    custom_session.connections.group.reconnect_backoff = 0.01
    fake_ws_server.step_refusals.append("Real-time data requires an upgrade")
    result = custom_session.query(FUTURES, ["ESZ5"]).blink()
    custom_session.wait(lambda s: s.opens == 2)  # moved to the delayed host, after a delay
    core = custom_session.session._core
    pooled = threading.Event()

    def read(*, executor: ThreadPoolExecutor, **_: object) -> tuple[pa.Table, int, float]:
        executor.submit(pooled.set).result(TIMEOUT)
        return core.catalog.dataset_schema(_DIVIDENDS).arrow_schema().empty_table(), 1, 600.0

    table = poll.open_poll(
        core,
        spec=_DIVIDENDS,
        request=Request(symbols=("AAPL",)),
        interval=timedelta(seconds=60),
        mode=dm.PollMode.REPLACE,
        query="<Query test>",
        read=read,
        pollers=poll.Pollers(core.label),
        host="api.test",
        path=_DIVIDENDS_PATH,
        clock=FakeClock(manual=True),
        stop=threading.Event(),
    )
    assert pooled.wait(TIMEOUT)
    # The host decision's reader would name a fan-out pool after the hosts.
    readers: list[str] = []

    def decide(*_: object, fetch: Callable[..., object], **__: object) -> HostDecision:
        readers.append(cast("Any", fetch).func.__self__.threads)
        return REALTIME

    monkeypatch.setattr(host_decision, "decide", decide)
    core.rest_transport().resolve_host(AssetClass.STOCKS, ChannelFamily.TICK)
    prefix, group = core.thread_prefix, custom_session.connections.group.label
    assert re.fullmatch(r"massive #\d+", prefix), prefix
    assert re.fullmatch(r"massive group #\d+", group), group
    assert readers == [f"{prefix} hosts rest"]
    mine = {name.removeprefix(f"{prefix} ") for name in started if name.startswith(f"{prefix} ")}
    shared = {name.removeprefix(f"{group} ") for name in started if name.startswith(f"{group} ")}
    assert len(mine) + len(shared) == len(set(started)), started
    assert {"diagnostics", "reaper"} <= mine
    assert {
        "ws 127.0.0.1/futures",  # the client of each connection, on the fake server
        "flush socket.test/futures",
        "move socket.test/futures",
        "start delayed.test/futures",
        "flush delayed.test/futures",
        "futures schedules",
    } <= shared
    polls = sorted(role for role in mine if role.startswith("poll-"))
    assert len(polls) == 2
    assert re.fullmatch(r"poll-\d+", polls[0])
    assert polls[1] == f"{polls[0]} rest_0"
    custom_session.session.release(result)
    custom_session.session.release(table)


# --- Failure delivery: every stream shape, every way its stream fails --------------------------
# Deephaven queues a blink table's failure until the next update cycle, and a table that hears
# of a second failure trips an assertion whose fatal-error handler exits the process. A stream
# fails through its blink table alone, once (`BlinkPublisher`), but every shape built on it
# (append-only, ring, last_by, a watchlist's join) and every table a user derives from a result
# is another node the failure passes through. The matrix crosses each shape a stream result
# takes with each way its stream fails: alone, twice in a row, racing a close or a release, and
# while a watchlist edit is on its way. Each case checks that the user's tables on the result
# end failed, that every table that failed heard of it exactly once, and that the process is
# still running.


WATCHED = "AAPL"
"""The symbol every matrix result streams, as the topic `T.AAPL`."""

ANCHOR = "MSFT"
"""The symbol of the quote stream each case opens first, so the connection is up, and its
reads can be held, before the result's own subscription is sent."""

ADDED = "NVDA"
"""The symbol a watchlist edit adds while the stream fails."""


@dataclasses.dataclass
class Live:
    """One case's result, the session's other results it holds, and the user's tables on it.

    Attributes:
        result: The result under test, a stream of `T.AAPL` trades.
        upstream: The session's other results: the anchor's quotes and, for a result that
            watches another stream, that stream's `last_by`.
        watchlist: The input table the result follows, directly or through the stream it
            watches; None for static symbols.
        derived: The user's tables on the result (the `user_tables` fixture).
        heard: Each table above, with the reader of the failures its listener heard.
        race: Runs calls at the same moment (the `race` fixture).
        cycles_ended: Waits until every queued failure is delivered (the `cycles_ended`
            fixture).
    """

    result: Table
    upstream: list[Table]
    watchlist: InputTable | None = None
    derived: list[Table] = dataclasses.field(default_factory=list)
    heard: list[tuple[Table, Callable[[], int]]] = dataclasses.field(default_factory=list)
    race: Callable[..., None] = dataclasses.field(init=False)
    cycles_ended: Callable[[], None] = dataclasses.field(init=False)


def quote(symbol: str) -> dict[str, object]:
    return {"ev": "Q", "sym": symbol, "bx": 4, "bp": 1.0, "bs": 100, "ax": 7, "ap": 1.1} | {
        "as": 100,
        "t": 1_700_000_000_000,
        "z": 3,
    }


def symbol_watchlist() -> InputTable:
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    watchlist.add(new_table([string_col("Symbol", [WATCHED])]))
    return watchlist


def static_shape(verb: Callable[[StreamQuery], Table]) -> Opener:
    def open_result(ws_session: WsSession, upstream: list[Table], _: InputTable | None) -> Live:
        return Live(verb(ws_session.query(TRADES, [WATCHED])), upstream)

    return open_result


def watchlist_shape(verb: Callable[[StreamQuery], Table]) -> Opener:
    def open_result(ws_session: WsSession, upstream: list[Table], _: InputTable | None) -> Live:
        watchlist = symbol_watchlist()
        return Live(verb(ws_session.query(TRADES, watchlist)), upstream, watchlist)

    return open_result


def watching_shape(
    ws_session: WsSession, upstream: list[Table], watchlist: InputTable | None
) -> Live:
    # Trades of the symbols another stream's last_by holds: the result's watchlist is a live
    # result of the session, which a close or a connection's failure fails with it.
    quotes = upstream[-1]
    return Live(ws_session.query(TRADES, quotes.view("Symbol")).last_by(), upstream, watchlist)


WATCHING = "last_by of another stream's symbols"

SHAPES: dict[str, Opener] = {
    "blink": static_shape(lambda q: q.blink()),
    "append_only": static_shape(lambda q: q.append_only()),
    "ring": static_shape(lambda q: q.ring(5)),
    "last_by": static_shape(lambda q: q.last_by()),
    "append_only of a watchlist": watchlist_shape(lambda q: q.append_only()),
    "last_by of a watchlist": watchlist_shape(lambda q: q.last_by()),
    WATCHING: watching_shape,
}
"""How each case opens its result, by test id."""


def open_live(
    ws_session: WsSession,
    shape: str,
    *,
    user_tables: UserTables,
    failures_heard: FailuresHeard,
    race: Callable[..., None],
    cycles_ended: Callable[[], None],
    refused: bool = False,
) -> Live:
    # Opens the anchor (and, for a watching result, the stream it watches, of a watchlist),
    # then the result and the user's tables on it, and listens to each. Under `refused` the
    # server's reads are held until then, so everything is built before Massive refuses the
    # topic.
    upstream = [ws_session.query(QUOTES, [ANCHOR]).blink()]
    ws_session.subscribed(f"Q.{ANCHOR}")
    watchlist = None
    if shape == WATCHING:
        watchlist = symbol_watchlist()
        quotes = ws_session.query(QUOTES, watchlist).last_by()
        ws_session.subscribed(f"Q.{WATCHED}")
        ws_session.publish(quote(WATCHED))
        wait_for(lambda: quotes.size == 1)
        upstream.append(quotes)
    if refused:
        ws_session.server.refuse[f"T.{WATCHED}"] = f"not authorized to access T.{WATCHED}"
        ws_session.server.hold_reads(held=True)
    live = SHAPES[shape](ws_session, upstream, watchlist)
    live.race = race
    live.cycles_ended = cycles_ended
    live.derived = user_tables(live.result, "Symbol", "Timestamp")
    live.heard = [(t, failures_heard(t)) for t in (live.result, *upstream, *live.derived)]
    if refused:
        ws_session.server.hold_reads(held=False)
    else:
        ws_session.subscribed(f"T.{WATCHED}")
    return live


def let_go(ws_session: WsSession, live: Live) -> None:
    # Releases the session's results; the user's tables and listeners go with their fixtures.
    for table in (live.result, *live.upstream):
        ws_session.session.release(table)


def reject_the_key(ws_session: WsSession) -> None:
    # The server drops the connection and rejects the key on the reconnect: a fatal failure
    # that fails every stream on the connection.
    ws_session.server.auth_replies.append("auth_failed")
    ws_session.server.close(1011)


def refuse_the_upgrade(ws_session: WsSession) -> None:
    # The server drops the connection and answers the reconnect's upgrade with HTTP 404, a
    # handshake that retrying cannot fix: every stream on the connection fails.
    ws_session.server.reject_next(404)
    ws_session.server.close(1011)


def connection_failure(fail: Callable[[WsSession], None]) -> Callable[[WsSession, Live], None]:
    def path(ws_session: WsSession, _live: Live) -> None:
        connection = ws_session.connection()
        fail(ws_session)
        ws_session.wait(lambda s: WsState.FAILED in s.states)
        assert connection.flusher.join(TIMEOUT)  # its last flush applied the failures

    return path


def topic_refused(ws_session: WsSession, _live: Live) -> None:
    ws_session.wait(lambda s: any(status.kind is StatusKind.REFUSED for status in s.statuses))


def closed(ws_session: WsSession, _live: Live) -> None:
    ws_session.session.close()


def then_closed(first: Callable[[WsSession, Live], None]) -> Callable[[WsSession, Live], None]:
    def path(ws_session: WsSession, live: Live) -> None:
        first(ws_session, live)
        wait_for(functools.partial(failed_after_flush, ws_session, live.result))
        live.cycles_ended()
        ws_session.session.close()

    return path


def close_racing_a_rejected_key(ws_session: WsSession, live: Live) -> None:
    live.race(ws_session.session.close, functools.partial(reject_the_key, ws_session))


def release_racing_a_rejected_key(ws_session: WsSession, live: Live) -> None:
    live.race(
        functools.partial(ws_session.session.release, live.result),
        functools.partial(reject_the_key, ws_session),
    )
    ws_session.wait(lambda s: WsState.FAILED in s.states)


PATHS: dict[str, tuple[Callable[[WsSession, Live], None], bool]] = {
    "topic refused": (topic_refused, False),
    "key rejected": (connection_failure(reject_the_key), True),
    "upgrade refused": (connection_failure(refuse_the_upgrade), True),
    "session closed": (closed, True),
    "topic refused, then session closed": (then_closed(topic_refused), True),
    "key rejected, then session closed": (then_closed(connection_failure(reject_the_key)), True),
    "close racing a rejected key": (close_racing_a_rejected_key, True),
    "release racing a rejected key": (release_racing_a_rejected_key, True),
}
"""Each way a case's stream fails, by test id, and whether the session's other results fail
with it (a refusal fails only the refused topic's streams)."""


def failed_once(ws_session: WsSession, live: Live, *, everything: bool) -> None:
    # The user's tables, and with `everything` the session's other results, end failed. Once
    # the update cycles that delivered the failures are over, every table that failed heard of
    # it exactly once, and every table that did not fail heard nothing.
    for table in [*live.derived, *(live.upstream if everything else ())]:
        wait_for(functools.partial(failed_after_flush, ws_session, table))
    live.cycles_ended()
    heard = [(bool(table.j_table.isFailed()), count()) for table, count in live.heard]
    assert [count for _, count in heard] == [int(failed) for failed, _ in heard]
    if not everything:
        assert not any(failed for failed, _ in heard[1 : 1 + len(live.upstream)])


@pytest.mark.parametrize("path", list(PATHS))
@pytest.mark.parametrize("shape", list(SHAPES))
def test_every_stream_shape_fails_once_by_every_path(
    ws_session: WsSession,
    shape: str,
    path: str,
    *,
    user_tables: UserTables,
    failures_heard: FailuresHeard,
    race: Callable[..., None],
    cycles_ended: Callable[[], None],
) -> None:
    fail, everything = PATHS[path]
    live = open_live(
        ws_session,
        shape,
        user_tables=user_tables,
        failures_heard=failures_heard,
        race=race,
        cycles_ended=cycles_ended,
        refused=path.startswith("topic refused"),
    )
    fail(ws_session, live)
    failed_once(ws_session, live, everything=everything)
    let_go(ws_session, live)


@pytest.mark.parametrize("path", ["key rejected", "session closed", "close racing a rejected key"])
@pytest.mark.parametrize("shape", [s for s in SHAPES if s.endswith("watchlist") or s == WATCHING])
def test_a_stream_failing_while_its_watchlist_changes_fails_once(
    ws_session: WsSession,
    shape: str,
    path: str,
    *,
    user_tables: UserTables,
    failures_heard: FailuresHeard,
    race: Callable[..., None],
    cycles_ended: Callable[[], None],
) -> None:
    # The edit adds a symbol and the stream fails before the subscription it asks for is sent:
    # the watch's change and the failure are on their way at once.
    fail, everything = PATHS[path]
    live = open_live(
        ws_session,
        shape,
        user_tables=user_tables,
        failures_heard=failures_heard,
        race=race,
        cycles_ended=cycles_ended,
    )
    assert live.watchlist is not None
    live.watchlist.add(new_table([string_col("Symbol", [ADDED])]))
    fail(ws_session, live)
    failed_once(ws_session, live, everything=everything)
    let_go(ws_session, live)


# --- Soak tests: long runs under load (marker `soak`, left out of a plain run) ----------------


class Soak(Protocol):
    # What the soak tests use of the `soak` fixture (tests/conftest.py).
    seconds: float
    running: threading.Event

    def rounds(self) -> Iterator[int]: ...

    def note(self, **figures: object) -> None: ...


SOAK_SYMBOLS = [f"S{i:02d}" for i in range(40)]
"""The symbols the soak feed trades."""

SOAK_WAIT = 180.0
"""Seconds a soak test waits for the rows still in flight when its feed stops, under load."""

_MODULUS = 1_000_000_007
"""A prime: each table sums its sequence numbers' squares modulo it, which stays in a long."""


def sequence_stats(table: Table, *, distinct: bool = False) -> Table:
    # Per symbol: the rows, the least and greatest sequence number, the sum of the numbers, the
    # sum of their squares modulo `_MODULUS`, and with `distinct` how many different numbers.
    # A table that received the numbers lo..hi of a symbol each exactly once has hi - lo + 1
    # rows and those two sums (`exactly_once`); a lost or repeated row breaks one of them, and
    # the state stays one row per symbol however long the run (`distinct` keeps every number,
    # so only a short-lived table asks for it). Built here, not in a test: a formula copies its
    # caller's locals into the JVM's query scope, and a test's locals hold its session.
    aggregations = [
        agg.count_("Rows"),
        agg.min_("Lo = Q"),
        agg.max_("Hi = Q"),
        agg.sum_(["Sum = Q", "Squares = Q2"]),
    ]
    if distinct:
        aggregations.append(agg.count_distinct("Distinct = Q"))
    return table.update_view(
        ["Q = SequenceNumber", f"Q2 = (Q % {_MODULUS}L) * (Q % {_MODULUS}L) % {_MODULUS}L"]
    ).agg_by(aggregations, by=["Symbol"])


def exactly_once(row: Mapping[str, Any]) -> bool:
    # Whether a `sequence_stats` row is that of the numbers Lo..Hi, each exactly once.
    lo, hi, rows = row["Lo"], row["Hi"], row["Rows"]
    squares = (hi * (hi + 1) * (2 * hi + 1) - (lo - 1) * lo * (2 * lo - 1)) // 6
    return bool(
        rows == hi - lo + 1
        and row["Sum"] == (lo + hi) * rows // 2
        and row["Squares"] % _MODULUS == squares % _MODULUS
    )


def by_symbol(to_pylist: ToPylist, stats: Table) -> dict[str, dict[str, Any]]:
    # A `sequence_stats` table's rows, by symbol.
    return {row["Symbol"]: row for row in to_pylist(stats)}


def all_exactly_once(to_pylist: ToPylist, stats: Table) -> bool:
    # Whether a `sequence_stats` table holds every symbol's numbers from 1 on, each once.
    return all(r["Lo"] == 1 and exactly_once(r) for r in by_symbol(to_pylist, stats).values())


def soak_wait(condition: Callable[[], bool], what: str) -> None:
    # Bounded wait for the rows a stopped feed left in flight to reach the tables.
    pause = threading.Event()  # never set; only paces the polling
    deadline = time.monotonic() + SOAK_WAIT
    while not condition():
        assert time.monotonic() < deadline, f"waited {SOAK_WAIT} s for {what}"
        pause.wait(0.1)


def soak_topics(server: FakeMassiveServer) -> set[str]:
    # The topics the latest connection holds subscribed on the fake server.
    with server._lock:
        return set(server.connections[-1].topics) if server.connections else set()


@pytest.mark.soak
@pytest.mark.jvm_held
def test_soak_every_stream_receives_every_row_of_a_fast_feed(
    make_ws_session: Callable[..., WsSession],
    *,
    make_trade_feed: Callable[..., TradeFeed],
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: ToPylist,
    soak: Soak,
) -> None:
    # Forty one-symbol streams, ten three-symbol streams and one of every symbol share one
    # connection while the feed sends trades as fast as the fake server writes them, and the
    # flush thread publishes every 50 ms. Every stream must end with every trade of its symbols,
    # each exactly once, and no table may fail.
    fake_ws_server.keep_frames = False
    harness = make_ws_session(flush_interval=0.05)
    core = harness.session._core
    wanted: list[list[str]] = [[symbol] for symbol in SOAK_SYMBOLS]
    wanted += [SOAK_SYMBOLS[i : i + 3] for i in range(0, 30, 3)]
    results = [harness.query(TRADES, symbols).blink() for symbols in wanted]
    results.append(harness.query(TRADES, dm.ALL).blink())
    wanted.append(SOAK_SYMBOLS)
    scope = new_scope()
    with scope.open():
        stats = [sequence_stats(result) for result in results]
    # The stream of every symbol subscribes the channel's wildcard, which covers the others.
    fake_ws_server.wait_for(lambda s: soak_topics(s) == {"T.*"})
    feed = make_trade_feed(SOAK_SYMBOLS)
    feed.start()
    started = time.monotonic()
    for _ in soak.rounds():
        assert not any(table.j_table.isFailed() for table in results)
        sent = sum(feed.sent().values())
        soak.note(rows=sent, rows_per_second=round(sent / (time.monotonic() - started)))
    feed.stop()
    by_sent = feed.sent()
    expected = [{symbol: by_sent[symbol] for symbol in symbols} for symbols in wanted]

    def counts() -> list[dict[str, int]]:
        return [{s: r["Rows"] for s, r in by_symbol(dh_to_pylist, t).items()} for t in stats]

    soak_wait(lambda: counts() == expected, "every stream to receive every row sent")
    assert all(all_exactly_once(dh_to_pylist, table) for table in stats)
    release_scope(core.exec_ctx, scope)
    for result in results:
        harness.session.release(result)


@pytest.mark.soak
@pytest.mark.jvm_held
def test_soak_streams_opened_and_released_under_load_lose_and_repeat_no_row(
    make_ws_session: Callable[..., WsSession],
    *,
    make_trade_feed: Callable[..., TradeFeed],
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: ToPylist,
    soak: Soak,
) -> None:
    # One stream of every symbol stays open while eight threads keep opening streams of one to
    # three symbols, reading them for a moment and releasing them, so the connection subscribes
    # and unsubscribes all the time under a fast feed. Every short stream must hold an unbroken
    # run of each of its symbols' trades, each once; the long one every trade; and once the
    # churn stops, only the long stream's topic stays subscribed.
    fake_ws_server.keep_frames = False
    harness = make_ws_session(flush_interval=0.05)
    core = harness.session._core
    every = harness.query(TRADES, dm.ALL).blink()
    scope = new_scope()
    with scope.open():
        every_stats = sequence_stats(every)
    fake_ws_server.wait_for(lambda s: "T.*" in soak_topics(s))
    feed = make_trade_feed(SOAK_SYMBOLS)
    feed.start()
    lock = threading.Lock()
    opened: list[int] = [0]
    problems: list[str] = []

    def churn(worker: int) -> None:
        # Each worker walks its own fixed sequence of one to three distinct symbols and stream
        # lifetimes, so a failure repeats on the next run.
        pause = threading.Event()  # never set; only paces the stream's life
        step = 0
        with entered(core.exec_ctx):
            while soak.running.is_set():
                step += 1
                symbols = [
                    SOAK_SYMBOLS[(worker * 5 + step * 7 + k * 11) % 40]
                    for k in range(1 + (worker + step) % 3)
                ]
                result = harness.query(TRADES, symbols).blink()
                own = new_scope()
                with own.open():
                    stats = sequence_stats(result)
                pause.wait((0.05, 0.12, 0.3, 0.08)[step % 4])
                rows = dh_to_pylist(stats)
                release_scope(core.exec_ctx, own)
                harness.session.release(result)
                bad = [r for r in rows if r["Symbol"] not in symbols or not exactly_once(r)]
                with lock:
                    opened[0] += 1
                    problems.extend(f"{symbols}: {row}" for row in bad)

    with ThreadPoolExecutor(8, thread_name_prefix="soak churn") as pool:
        workers = [pool.submit(churn, worker) for worker in range(8)]
        try:
            for _ in soak.rounds():
                with lock:
                    assert not problems, problems[:5]
                    soak.note(streams_opened=opened[0], rows=sum(feed.sent().values()))
        finally:
            soak.running.clear()  # a failed round stops the workers too
            for worker in workers:
                worker.result(SOAK_WAIT)
    feed.stop()
    assert not problems, problems[:5]
    sent = feed.sent()
    soak_wait(
        lambda: {s: r["Rows"] for s, r in by_symbol(dh_to_pylist, every_stats).items()} == sent,
        "the stream of every symbol to receive every row sent",
    )
    assert all_exactly_once(dh_to_pylist, every_stats)
    soak_wait(lambda: soak_topics(fake_ws_server) == {"T.*"}, "the short streams' topics to go")
    release_scope(core.exec_ctx, scope)
    harness.session.release(every)


def edit_watchlist(watchlist: InputTable, soak: Soak, edits: list[int]) -> None:
    # Edit a watchlist until the run ends, on a thread of the test's: a fixed sequence of three
    # adds and then three deletes, walking the symbols, 10 to 50 ms apart.
    pause = threading.Event()  # never set; only paces the edits
    while soak.running.is_set():
        step = edits[0]
        change = new_table([string_col("Symbol", [SOAK_SYMBOLS[(step * 7) % 40]])])
        if (step // 3) % 2 == 0:
            watchlist.add(change)
        else:
            watchlist.delete(change)
        edits[0] += 1
        pause.wait((0.01, 0.03, 0.05)[step % 3])


def hold_exactly(watchlist: InputTable, symbols: list[str], to_pylist: ToPylist) -> None:
    # Make a keyed watchlist hold exactly `symbols`; deletes take a static key table.
    held = [row["Symbol"] for row in to_pylist(watchlist)]
    if held:
        watchlist.delete(new_table([string_col("Symbol", held)]))
    watchlist.add(new_table([string_col("Symbol", symbols)]))


@pytest.mark.soak
@pytest.mark.jvm_held
def test_soak_a_stream_follows_a_churning_watchlist_under_load(
    make_ws_session: Callable[..., WsSession],
    *,
    make_trade_feed: Callable[..., TradeFeed],
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: ToPylist,
    soak: Soak,
) -> None:
    # A thread keeps adding symbols to a watchlist and deleting them while the feed runs. Within
    # each round the stream must never hold a trade twice; once the watchlist stops changing,
    # the stream must subscribe exactly its symbols and receive their trades, and only theirs.
    fake_ws_server.keep_frames = False
    harness = make_ws_session(flush_interval=0.05)
    core = harness.session._core
    final = SOAK_SYMBOLS[:10]
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    watchlist.add(new_table([string_col("Symbol", final)]))
    result = harness.query(TRADES, watchlist).blink()
    fake_ws_server.wait_for(lambda s: f"T.{final[0]}" in soak_topics(s))
    feed = make_trade_feed(SOAK_SYMBOLS)
    feed.start()
    edits: list[int] = [0]

    def edit() -> None:
        with entered(core.exec_ctx):
            edit_watchlist(watchlist, soak, edits)

    with ThreadPoolExecutor(1, thread_name_prefix="soak watchlist") as pool:
        editor = pool.submit(edit)
        try:
            for _ in soak.rounds():
                # Each round counts its own rows anew, so the distinct count stays small.
                own = new_scope()
                with own.open():
                    stats = sequence_stats(result, distinct=True)
                threading.Event().wait(1.5)  # never set; only paces the round
                counted = dh_to_pylist(stats)
                release_scope(core.exec_ctx, own)
                assert all(r["Distinct"] == r["Rows"] for r in counted), counted
                soak.note(edits=edits[0], rows=sum(feed.sent().values()))
        finally:
            soak.running.clear()
            editor.result(SOAK_WAIT)
    hold_exactly(watchlist, final, dh_to_pylist)
    topics = {f"T.{symbol}" for symbol in final}
    soak_wait(lambda: soak_topics(fake_ws_server) == topics, "the final watchlist's topics")
    scope = new_scope()
    with scope.open():
        stats = sequence_stats(result)
    soak_wait(lambda: set(by_symbol(dh_to_pylist, stats)) == set(final), "every final symbol")
    feed.stop()
    sent = feed.sent()
    soak_wait(
        lambda: all(r["Hi"] == sent[s] for s, r in by_symbol(dh_to_pylist, stats).items()),
        "the final symbols' last rows",
    )
    received = by_symbol(dh_to_pylist, stats)
    assert set(received) == set(final)
    assert all(exactly_once(row) for row in received.values())
    release_scope(core.exec_ctx, scope)
    harness.session.release(result)


def close_under_load(harness: WsSession, round_: int) -> float:
    # One round of the close soak: a second session joins the connection, opens twenty streams,
    # and four threads keep opening and releasing more of its streams; then four threads close
    # it at once. Returns how long the slowest close took, in seconds.
    other = join(harness)
    symbols = [SOAK_SYMBOLS[(round_ + i) % 40] for i in range(20)]
    results = [harness.query(TRADES, [symbol], other).blink() for symbol in symbols]
    stop = threading.Event()
    errors: list[str] = []
    timings: list[float] = []
    closing = threading.Barrier(5)

    def churn(worker: int) -> None:
        step = 0
        while not stop.is_set():
            step += 1
            try:
                result = harness.query(TRADES, [SOAK_SYMBOLS[(worker * 3 + step) % 40]], other)
                other.release(result.blink())
            except dm.MassiveError as error:  # a verb that races the close may find it closed
                if " is closed" not in str(error) and " was closed" not in str(error):
                    errors.append(str(error))
                return

    def close() -> None:
        closing.wait(TIMEOUT)
        began = time.monotonic()
        other.close()
        timings.append(time.monotonic() - began)

    with ThreadPoolExecutor(8, thread_name_prefix="soak close") as pool:
        churners = [pool.submit(churn, worker) for worker in range(4)]
        closers = [pool.submit(close) for _ in range(4)]
        threading.Event().wait(0.5)  # never set; lets the churn run under the feed first
        closing.wait(TIMEOUT)
        for closer in closers:
            closer.result(TIMEOUT)
        stop.set()
        for churner in churners:
            churner.result(TIMEOUT)
    assert not errors, errors[:5]
    wait_for(lambda: all(result.j_table.isFailed() for result in results))
    return max(timings)


@pytest.mark.soak
@pytest.mark.jvm_held
def test_soak_sessions_close_under_load_without_disturbing_a_shared_connection(
    make_ws_session: Callable[..., WsSession],
    *,
    make_trade_feed: Callable[..., TradeFeed],
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: ToPylist,
    soak: Soak,
) -> None:
    # Each round, a second session on the same connection is closed by four threads at once
    # while its streams churn and the feed runs (`close_under_load`). Every close must return
    # promptly, every verb racing it must succeed or say the session is closed, every table it
    # made must fail, and the first session's stream of every symbol, on that connection all
    # along, must still receive every trade exactly once.
    fake_ws_server.keep_frames = False
    harness = make_ws_session(flush_interval=0.05)
    core = harness.session._core
    every = harness.query(TRADES, dm.ALL).blink()
    scope = new_scope()
    with scope.open():
        every_stats = sequence_stats(every)
    fake_ws_server.wait_for(lambda s: soak_topics(s) == {"T.*"})
    feed = make_trade_feed(SOAK_SYMBOLS)
    feed.start()
    slowest: list[float] = []
    for round_ in soak.rounds():
        slowest.append(close_under_load(harness, round_))
        soak.note(sessions_closed=len(slowest), slowest_close_seconds=round(max(slowest), 2))
    feed.stop()
    sent = feed.sent()
    soak_wait(
        lambda: {s: r["Rows"] for s, r in by_symbol(dh_to_pylist, every_stats).items()} == sent,
        "the first session's stream to receive every row sent",
    )
    assert all_exactly_once(dh_to_pylist, every_stats)
    release_scope(core.exec_ctx, scope)
    harness.session.release(every)
