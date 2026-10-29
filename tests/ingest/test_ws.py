"""Tests for the WebSocket transport: the reconcile loop, connections, and live streams.

`Subscriptions` is tested alone, with a recording sender and a manual timer queue, so every step
of the reconcile loop is deterministic. Connections and streams are tested end to end, on the
`ws_session` fixture (`tests/_fakes/ws_session.py`): a real session whose `WsConnections`
reaches the fake Massive server through real `WsClient`s, with every client callback recorded
so a test waits for exactly the event it needs. Flushes are explicit (`WsSession.flush`): the
flush interval is an hour unless a test needs the flush thread itself.
"""

from __future__ import annotations

import gc
import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any, cast

import pytest
from deephaven import dtypes, input_table, new_table
from deephaven.column import string_col

import deephaven_massive as dm
from deephaven_massive import _massive
from deephaven_massive._datasets.catalog import DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._datasets.request import HostDecision
from deephaven_massive._enums import AssetClass, ChannelFamily, Recency
from deephaven_massive._ingest import futures_calendar, ws
from deephaven_massive._massive import Status, StatusKind, Transport, WsState
from deephaven_massive._tables.diagnostics import StreamUpdate
from deephaven_massive._tables.liveness import new_scope
from deephaven_massive._tables.stream import open_stream_tables
from deephaven_massive._tables.watchlist import SymbolWatch

# Plain constants of the harness, which no fixture can hand over; sessions come from fixtures.
from tests._fakes.ws_session import REALTIME, TIMEOUT

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import TypeAlias

    from deephaven.table import Table

    from tests._fakes.ws import FakeMassiveServer
    from tests._fakes.ws_session import WsSession


RowsWhen: TypeAlias = (
    "Callable[[Table, Callable[[list[dict[str, Any]]], bool]], list[dict[str, Any]]]"
)
W = Transport.WS


# --- The datasets the end-to-end tests stream -------------------------------------------------

STOCK_KEYS = SourceKeys(symbol=KeySpec("Ticker"), time=KeySpec("SipTimestamp"))
TRADES = DatasetSpec(
    name="test.trades",
    summary="A test dataset.",
    asset_class=AssetClass.STOCKS,
    channels={None: "T"},
    keys={(W, "stocks/T"): STOCK_KEYS},
)
QUOTES = DatasetSpec(
    name="test.quotes",
    summary="A test dataset.",
    asset_class=AssetClass.STOCKS,
    channels={None: "Q"},
    keys={(W, "stocks/Q"): STOCK_KEYS},
)
FOREX = DatasetSpec(
    name="test.forex",
    summary="A test dataset.",
    asset_class=AssetClass.FOREX,
    channels={None: "C"},
    keys={(W, "forex/C"): SourceKeys(KeySpec("Ticker"), KeySpec("ParticipantTimestamp"))},
)
FOREX_FMV = DatasetSpec(
    name="test.forex_fmv",
    summary="A test dataset.",
    asset_class=AssetClass.FOREX,
    channels={None: "FMV"},
    family=ChannelFamily.FMV,
    keys={(W, "forex/FMV"): SourceKeys(KeySpec("Ticker"))},
)
OPTIONS = DatasetSpec(
    name="test.options",
    summary="A test dataset.",
    asset_class=AssetClass.OPTIONS,
    channels={None: "T"},
    keys={(W, "options/T"): SourceKeys(KeySpec("Ticker"), KeySpec("SipTimestamp"))},
)
OPTION_QUOTES = DatasetSpec(
    name="test.option_quotes",
    summary="A test dataset.",
    asset_class=AssetClass.OPTIONS,
    channels={None: "Q"},
    keys={(W, "options/Q"): SourceKeys(KeySpec("Ticker"), KeySpec("SipTimestamp"))},
)
FUTURES = DatasetSpec(
    name="test.futures",
    summary="A test dataset.",
    asset_class=AssetClass.FUTURES,
    channels={None: "T"},
    keys={(W, "futures/T"): SourceKeys(KeySpec("Ticker"), KeySpec("Timestamp"))},
)

QUOTES_FUTURES = DatasetSpec(
    name="test.futures_quotes",
    summary="A test dataset.",
    asset_class=AssetClass.FUTURES,
    channels={None: "Q"},
    keys={(W, "futures/Q"): SourceKeys(KeySpec("Ticker"), KeySpec("Timestamp"))},
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


def streams_of(
    ws_session: WsSession, dh_to_pylist: Callable[[Table], list[dict[str, Any]]]
) -> list[dict[str, Any]]:
    assert ws_session.session._core.diagnostics.barrier().wait(TIMEOUT)
    return sorted(dh_to_pylist(ws_session.session.streams()), key=lambda r: r["StreamId"])


def connections_of(
    ws_session: WsSession, dh_to_pylist: Callable[[Table], list[dict[str, Any]]]
) -> list[dict[str, Any]]:
    assert ws_session.session._core.diagnostics.barrier().wait(TIMEOUT)
    return [r for r in dh_to_pylist(ws_session.session.connections()) if r["Transport"] == "WS"]


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
    second = ws_session.query(TRADES, ["aapl"]).last_by()
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
    assert "socket.test/stocks refused Q.AAPL: not authorized to access Q.AAPL" in rows[1]["Error"]
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


def test_a_rejected_row_fails_only_the_streams_of_its_symbol(
    ws_session: WsSession,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    *,
    rows_when: RowsWhen,
) -> None:
    aapl = ws_session.query(TRADES, ["AAPL"]).append_only()
    msft = ws_session.query(TRADES, ["MSFT"]).blink()
    every = ws_session.query(TRADES, dm.ALL).blink()
    ws_session.subscribed("T.AAPL", "T.MSFT", "T.*")
    bad = trade("MSFT")
    del bad["t"]  # a required key
    ws_session.send(bad, trade("AAPL"))
    ws_session.flush()
    rows_when(aapl, lambda rows: len(rows) == 1)
    rows = streams_of(ws_session, dh_to_pylist)
    assert [r["State"] for r in rows] == ["RUNNING", "FAILED", "FAILED"]
    assert "no SipTimestamp" in rows[1]["Error"]
    del msft, every


def test_a_row_without_a_symbol_fails_only_the_all_streams(
    ws_session: WsSession,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    aapl = ws_session.query(TRADES, ["AAPL"]).blink()
    every = ws_session.query(TRADES, dm.ALL).blink()
    ws_session.subscribed("T.AAPL", "T.*")
    nameless = trade("AAPL")
    del nameless["sym"]
    unreadable = trade("not a symbol")
    del unreadable["t"]
    with caplog.at_level(logging.ERROR, logger=ws.__name__):
        ws_session.send(nameless, unreadable)
    ws_session.flush()
    rows = streams_of(ws_session, dh_to_pylist)
    assert [r["State"] for r in rows] == ["RUNNING", "FAILED"]
    assert caplog.text.count("has no valid symbol; failing the channel's ALL streams") == 2
    del aapl, every


def test_a_timestamp_out_of_bounds_fails_only_its_streams(
    ws_session: WsSession,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    *,
    rows_when: RowsWhen,
) -> None:
    aapl = ws_session.query(TRADES, ["AAPL"]).append_only()
    msft = ws_session.query(TRADES, ["MSFT"]).blink()
    ws_session.subscribed("T.AAPL", "T.MSFT")
    ws_session.send(trade("MSFT", t=5), trade("AAPL"))
    ws_session.flush()
    rows_when(aapl, lambda rows: len(rows) == 1)
    rows = streams_of(ws_session, dh_to_pylist)
    assert [r["State"] for r in rows] == ["RUNNING", "FAILED"]
    assert "timestamp unit mismatch for test.trades.Timestamp: 5 ms" in rows[1]["Error"]
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
    assert stream["SchemaDrift"] == "WS:zz+"
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
        derived = [result.where("Price > 0")]
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


def test_close_returns_promptly_during_a_backoff_wait(
    fake_ws_server: FakeMassiveServer, make_ws_session: Callable[..., WsSession]
) -> None:
    custom_session = make_ws_session(backoff=60.0)
    fake_ws_server.reject_next(503)
    result = custom_session.query(TRADES, ["AAPL"]).blink()
    custom_session.wait(lambda s: WsState.RECONNECTING in s.states)
    started = time.monotonic()
    custom_session.close()
    assert time.monotonic() - started < 5.0
    assert WsState.CLOSED in custom_session.states
    del result


def test_a_rejected_key_fails_every_stream(
    ws_session: WsSession,
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    q = ws_session.query(TRADES, ["AAPL"])
    result = q.blink()
    ws_session.subscribed("T.AAPL")  # attached: the failure below cannot race the open
    failed = ws_session.connection()
    fake_ws_server.auth_replies.append("auth_failed")  # for the reconnect
    fake_ws_server.close(1011)
    ws_session.wait(lambda s: WsState.FAILED in s.states)
    with ws_session.session._core.registry_lock:
        assert failed.retired
        assert ws_session.connections._connections == {}  # retired: the next stream opens anew
    assert failed.flusher.join(TIMEOUT)  # its last flush applied the failures, unasked
    [row] = connections_of(ws_session, dh_to_pylist)
    assert row["State"] == "FAILED"
    assert "rejected the API key" in row["LastError"]
    [stream] = streams_of(ws_session, dh_to_pylist)
    assert stream["State"] == "FAILED"
    again = q.blink()  # a fresh stream, on a fresh connection
    assert len(ws_session.urls) == 2
    del result, again


def test_a_fatal_handshake_names_the_status_and_host(
    ws_session: WsSession,
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.subscribed("T.AAPL")  # attached: the failure below cannot race the open
    fake_ws_server.reject_next(404)  # for the reconnect
    fake_ws_server.close(1011)
    ws_session.wait(lambda s: WsState.FAILED in s.states)
    [row] = connections_of(ws_session, dh_to_pylist)
    assert row["LastError"].startswith("the WebSocket connection to socket.test/stocks failed:")
    assert "404" in row["LastError"]
    del result


def test_a_reconnect_resends_the_desired_set(
    ws_session: WsSession,
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.subscribed("T.AAPL")
    fake_ws_server.close(1011)
    ws_session.wait(lambda s: s.opens == 2)
    ws_session.subscribed("T.AAPL")  # on the new connection
    ws_session.wait(lambda s: sum(status.kind is StatusKind.ACK for status in s.statuses) == 2)
    [row] = connections_of(ws_session, dh_to_pylist)
    assert (row["State"], row["Reconnects"], row["Topics"], row["AckedTopics"]) == (
        "CONNECTED",
        1,
        1,
        1,
    )
    assert WsState.RECONNECTING in ws_session.states
    ws_session.session.release(result)


def test_an_idle_connection_closes_and_a_new_stream_opens_another(
    ws_session: WsSession,
    monkeypatch: pytest.MonkeyPatch,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    monkeypatch.setattr(ws, "IDLE_GRACE", 0.05)
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.subscribed("T.AAPL")
    ws_session.session.release(result)
    ws_session.wait(lambda s: WsState.CLOSED in s.states)
    [row] = connections_of(ws_session, dh_to_pylist)
    assert row["State"] == "CLOSED"
    again = ws_session.query(TRADES, ["MSFT"]).blink()
    ws_session.subscribed("T.MSFT")
    assert len(ws_session.urls) == 2
    ws_session.session.release(again)


def test_an_opening_stream_cancels_the_idle_close(ws_session: WsSession) -> None:
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    connection = ws_session.connection()
    ws_session.subscribed("T.AAPL")
    ws_session.session.release(result)
    ws_session.unsubscribed("T.AAPL")
    with ws_session.session._core.registry_lock:
        connection.leases += 1  # a stream is opening
    retired = ws_session.on_loop(
        connection, lambda: ws_session.connections.retire(connection, idle=True)
    )
    assert retired is False
    with ws_session.session._core.registry_lock:
        connection.leases -= 1


def test_a_missing_acknowledgement_reconnects(
    fake_ws_server: FakeMassiveServer, make_ws_session: Callable[..., WsSession]
) -> None:
    custom_session = make_ws_session(ack_timeout=0.05)
    fake_ws_server.silent.add("T.AAPL")
    result = custom_session.query(TRADES, ["AAPL"]).blink()
    custom_session.wait(lambda s: s.opens >= 2)
    assert [json.loads(f)["params"] for f in fake_ws_server.frames(0)] == ["T.AAPL", "T.AAPL"]
    del result


def test_a_reactive_table_follows_its_rows(
    ws_session: WsSession,
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    *,
    rows_when: RowsWhen,
) -> None:
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    watchlist.add(new_table([string_col("Symbol", ["aapl", "not a symbol!"])]))
    result = ws_session.query(TRADES, watchlist).append_only()
    ws_session.subscribed("T.AAPL")
    ws_session.send(trade("AAPL"), trade("MSFT"))
    ws_session.flush()
    rows_when(result, lambda rows: len(rows) == 1)
    watchlist.add(new_table([string_col("Symbol", ["MSFT"])]))
    ws_session.subscribed("T.MSFT")
    watchlist.delete(new_table([string_col("Symbol", ["aapl"])]))
    ws_session.unsubscribed("T.AAPL")
    ws_session.send(trade("AAPL", 2.0), trade("MSFT", 3.0))
    ws_session.flush()
    shown = rows_when(result, lambda rows: len(rows) == 2)
    assert [r["Symbol"] for r in shown] == ["AAPL", "MSFT"]
    # The row that is no valid symbol never became a topic.
    sent = [json.loads(f)["params"] for f in fake_ws_server.frames()]
    assert sent == ["T.AAPL", "T.MSFT", "T.AAPL"]


def test_a_derived_table_keeps_following_the_watchlist(
    ws_session: WsSession, rows_when: RowsWhen
) -> None:
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    watchlist.add(new_table([string_col("Symbol", ["AAPL"])]))
    result = ws_session.query(TRADES, watchlist).append_only()
    ws_session.subscribed("T.AAPL")
    derived = result.where("Price > 0")
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


def test_the_real_time_verdict_moves_the_connection_once(
    fake_ws_server: FakeMassiveServer,
    make_ws_session: Callable[..., WsSession],
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    custom_session = make_ws_session(decide=unknown_futures)
    custom_session.connections._reconnect_backoff = 0.01
    fake_ws_server.step_refusals.append("Real-time data requires an upgrade")
    result = custom_session.query(FUTURES, ["ESZ5"]).blink()
    custom_session.wait(lambda s: s.opens == 2)
    assert custom_session.urls == ["wss://socket.test/futures", "wss://delayed.test/futures"]
    custom_session.subscribed("T.ESZ5")
    [row] = connections_of(custom_session, dh_to_pylist)
    assert (row["Host"], row["Recency"]) == ("delayed.test", "DELAYED")
    assert row["Evidence"] == "server verdict: Real-time data requires an upgrade"
    [stream] = streams_of(custom_session, dh_to_pylist)
    assert stream["State"] == "RUNNING"
    assert custom_session.verdicts == [
        (
            AssetClass.FUTURES,
            ChannelFamily.TICK,
            HostDecision(
                "delayed.test",
                Recency.DELAYED,
                "server verdict: Real-time data requires an upgrade",
            ),
        )
    ]
    assert stream["Host"] == "delayed.test"  # the stream's row follows the move
    other = custom_session.query(FUTURES, ["NQZ5"]).blink()  # the recorded verdict decides
    assert custom_session.urls == ["wss://socket.test/futures", "wss://delayed.test/futures"]
    custom_session.session.release(result)
    custom_session.session.release(other)


def realtime_futures(*_: object) -> HostDecision:
    return HostDecision("socket.test", Recency.REALTIME, "timeframe=REAL-TIME")


@pytest.mark.parametrize(
    ("decide", "pinned"),
    [(unknown_futures, {AssetClass.FUTURES: "socket.test"}), (realtime_futures, {})],
)
def test_a_verdict_otherwise_fails_the_step(
    fake_ws_server: FakeMassiveServer,
    make_ws_session: Callable[..., WsSession],
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    *,
    decide: Callable[[AssetClass, ChannelFamily], HostDecision],
    pinned: dict[AssetClass, str],
) -> None:
    custom_session = make_ws_session(decide=decide, ws_hosts=pinned)
    fake_ws_server.step_refusals.append("Real-time data requires an upgrade")
    result = custom_session.query(FUTURES, ["ESZ5"]).blink()
    custom_session.wait(lambda s: any(status.kind is StatusKind.REFUSED for status in s.statuses))
    custom_session.connection("wss://socket.test/futures").flusher.flush()
    [stream] = streams_of(custom_session, dh_to_pylist)
    assert stream["State"] == "FAILED"
    assert len(custom_session.urls) == 1
    del result


def test_a_failed_open_returns_its_lease(
    ws_session: WsSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    def broken(*_: object, **__: object) -> object:
        raise RuntimeError("no tables")

    monkeypatch.setattr(ws, "open_stream_tables", broken)
    with pytest.raises(RuntimeError, match="no tables"):
        ws_session.query(TRADES, ["AAPL"]).blink()
    assert ws_session.connection().leases == 0
    assert ws_session.connections._streams == {}


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


def test_a_lost_race_closes_the_spare_connection(
    ws_session: WsSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    barrier = threading.Barrier(2)
    built: list[ws.Connection] = []
    built_lock = threading.Lock()
    real: Callable[..., ws.Connection] = ws.Connection

    def racing(*args: object, **kwargs: object) -> ws.Connection:
        connection = real(*args, **kwargs)
        with built_lock:
            built.append(connection)
        barrier.wait(TIMEOUT)
        return connection

    monkeypatch.setattr(ws, "Connection", racing)
    with ThreadPoolExecutor(2) as pool:
        results = list(pool.map(lambda s: ws_session.query(TRADES, [s]).blink(), ["AAPL", "MSFT"]))
    assert len(built) == 2
    assert len(ws_session.connections._connections) == 1
    spare = next(c for c in built if c not in ws_session.connections._connections.values())
    assert spare.join(TIMEOUT)
    for result in results:
        ws_session.session.release(result)


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
    with caplog.at_level(logging.INFO, logger=ws.__name__):
        ws_session.session.release(result)
        wait_for(lambda: "has closed; dropped release_stream" in caplog.text)


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


def spare_connection(ws_session: WsSession, host: str) -> ws.Connection:
    return ws.Connection(
        ws_session.connections,
        ws_session.session._core,
        url=f"wss://{host}/stocks",
        asset_class=AssetClass.STOCKS,
        family=ChannelFamily.TICK,
        decision=REALTIME,
        client_factory=ws_session.factory,
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


def test_changes_for_a_moved_stream_follow_it(ws_session: WsSession) -> None:
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    result = ws_session.query(TRADES, watchlist).blink()
    old = ws_session.connection()
    successor = spare_connection(ws_session, "next.test")
    successor.start()
    with ws_session.session._core.registry_lock:
        ws_session.connections._streams[1] = successor
        successor.awaiting.add(1)
    ws_session.on_loop(old, lambda: old.hand_over(successor))
    assert old.join(TIMEOUT)
    # The old loop has ended, so its methods run here, as a late callback would have there.
    old.change_symbols(1, frozenset({"AAPL"}), frozenset())
    ws_session.subscribed("T.AAPL")  # on the successor
    old.release_stream(1)
    ws_session.unsubscribed("T.AAPL")
    successor.close()
    del result


def test_a_release_that_overtakes_the_adoption_waits_for_it(ws_session: WsSession) -> None:
    # The shutdown callback read the moved lease and posted before the old loop's hand-over.
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.subscribed("T.AAPL")
    old = ws_session.connection()
    successor = spare_connection(ws_session, "next.test")
    successor.start()
    with ws_session.session._core.registry_lock:
        ws_session.connections._streams[1] = successor
        successor.awaiting.add(1)
    successor.post(successor.release_stream, 1)
    successor.post(successor.change_symbols, 2, frozenset(), frozenset())  # not awaited: dropped
    ws_session.on_loop(old, lambda: old.hand_over(successor))
    assert old.join(TIMEOUT)
    ws_session.on_loop(successor, lambda: None)  # the adoption ran
    assert ws_session.on_loop(successor, lambda: successor.subscriptions.wanted) is False
    assert ws_session.on_loop(successor, lambda: dict(successor._streams)) == {}
    successor.close()
    assert successor.join(TIMEOUT)
    del result


def test_a_hand_over_to_a_failed_successor_fails_the_streams(
    ws_session: WsSession, dh_to_pylist: Callable[[Table], list[dict[str, Any]]]
) -> None:
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.subscribed("T.AAPL")
    old = ws_session.connection()
    successor = spare_connection(ws_session, "next.test")
    with ws_session.session._core.registry_lock:
        successor.failure = dm.MassiveError("rejected the API key")
    ws_session.on_loop(old, lambda: old.hand_over(successor))
    assert old.join(TIMEOUT)  # its last flush failed the stream
    [row] = streams_of(ws_session, dh_to_pylist)
    assert (row["State"], row["Error"]) == ("FAILED", "rejected the API key")
    successor.close()
    assert successor.join(TIMEOUT)
    del result


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
    wait_for(lambda: not ws_session.connections._streams)
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


def test_last_message_rows_are_throttled(
    ws_session: WsSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.subscribed("T.AAPL")
    connection = ws_session.connection()
    written: list[int] = []
    monkeypatch.setattr(connection, "_write_row", lambda: written.append(1))
    ws_session.send(trade("AAPL"))
    ws_session.send(trade("AAPL"))
    assert len(written) <= 1
    ws_session.session.release(result)


def test_an_index_value_refusal_names_the_business_host(ws_session: WsSession) -> None:
    connection = ws.Connection(
        ws_session.connections,
        ws_session.session._core,
        url="wss://socket.test/indices",
        asset_class=AssetClass.INDICES,
        family=ChannelFamily.TICK,
        decision=REALTIME,
        client_factory=ws_session.factory,
    )
    connection._refused(("V.I:SPX",), "not authorized")
    assert connection._last_error is not None
    assert "index values may need business.massive.com" in connection._last_error
    assert "ws_hosts[AssetClass.INDICES]" in connection._last_error
    connection.close()
    assert connection.join(TIMEOUT)


def test_a_connection_whose_session_is_gone_writes_nothing(
    ws_session: WsSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    result = ws_session.query(OPTIONS, ["O:SPY251219C00600000"]).blink()
    ws_session.subscribed("T.O:SPY251219C00600000")
    connection = ws_session.connection("wss://socket.test/options")
    monkeypatch.setattr(connection, "_core", lambda: None)
    monkeypatch.setattr(ws_session.connections, "_core", lambda: None)
    ws_session.on_loop(connection, connection._write_row)
    ws_session.on_loop(connection, connection._show_deferred)
    drift = _massive.DriftReport(extra=frozenset({"zz"}))
    ws_session.on_loop(connection, lambda: connection._record_drift("T", drift))
    ws_session.on_loop(
        connection, lambda: connection._update_streams({1: StreamUpdate(deferred=1)})
    )
    decision = HostDecision("delayed.test", Recency.DELAYED, "server verdict: x")
    ws_session.connections._move(connection, decision)  # the session is gone: nothing moves
    ws_session.connections._shut_down(1)
    assert ws_session.on_loop(
        connection, lambda: ws_session.connections.retire(connection, idle=False)
    )
    assert ws_session.connections.reroute(connection, "real-time") is False
    connection.close()  # retired: the session's close no longer reaches it
    assert connection.join(TIMEOUT)
    del result


def test_retiring_a_replaced_connection_keeps_its_replacement(ws_session: WsSession) -> None:
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    connection = ws_session.connection()
    spare = spare_connection(ws_session, "spare.test")
    assert ws_session.connections.retire(spare, idle=False)
    assert ws_session.connection() is connection
    spare.close()
    assert spare.join(TIMEOUT)
    ws_session.session.release(result)


def test_a_verdict_is_taken_once(make_ws_session: Callable[..., WsSession]) -> None:
    custom_session = make_ws_session(decide=unknown_futures)
    custom_session.connections._reconnect_backoff = 0.01
    result = custom_session.query(FUTURES, ["ESZ5"]).blink()
    other = custom_session.query(TRADES, ["AAPL"]).blink()  # a stream the move leaves alone
    connection = custom_session.connection("wss://socket.test/futures")
    with custom_session.session._core.registry_lock:  # a fatal failure retired it meanwhile
        del custom_session.connections._connections[connection.url]
    assert custom_session.on_loop(
        connection, lambda: custom_session.connections.reroute(connection, "x")
    )
    assert custom_session.connections.reroute(connection, "x") is False  # once per connection
    custom_session.wait(lambda s: s.urls[-1] == "wss://delayed.test/futures")
    wait_for(lambda: "wss://delayed.test/futures" in custom_session.connections._connections)
    with custom_session.session._core.registry_lock:
        owners = dict(custom_session.connections._streams)
    assert owners[1].url == "wss://delayed.test/futures"
    assert owners[2].url == "wss://socket.test/stocks"
    assert len(custom_session.verdicts) == 1
    custom_session.session.release(result)
    custom_session.session.release(other)


def test_a_verdict_that_cannot_be_recorded_still_moves(
    make_ws_session: Callable[..., WsSession], caplog: pytest.LogCaptureFixture
) -> None:
    custom_session = make_ws_session(decide=unknown_futures)

    def broken(*_: object) -> None:
        raise RuntimeError("no REST transport")

    custom_session.connections._reconnect_backoff = 0.01
    custom_session.connections._record_verdict = broken
    result = custom_session.query(FUTURES, ["ESZ5"]).blink()
    connection = custom_session.connection("wss://socket.test/futures")
    assert custom_session.on_loop(
        connection, lambda: custom_session.connections.reroute(connection, "x")
    )
    custom_session.wait(lambda s: s.urls[-1] == "wss://delayed.test/futures")
    assert "could not record the real-time verdict" in caplog.text
    custom_session.session.release(result)


def test_a_connection_that_does_not_end_is_reported(
    ws_session: WsSession, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    connection = ws_session.connection()
    monkeypatch.setattr(ws, "JOIN_TIMEOUT", 0.01)
    monkeypatch.setattr(connection, "join", lambda _: False)
    ws_session.connections.close()
    assert "has not ended 0 s after close()" in caplog.text
    del result


def test_the_default_factory_builds_real_clients(ws_session: WsSession) -> None:
    factory = ws._WsClientFactory("test-key", backoff=15.0, max_backoff=30.0)
    client = factory(
        "wss://socket.test/stocks",
        sources=[],
        on_open=lambda *_: None,
        on_status=lambda *_: None,
        on_data=lambda *_: None,
        on_state=lambda *_: None,
    )
    assert isinstance(client, _massive.WsClient)
    client.close()
    client.start()
    assert client.join(TIMEOUT)
    assert "test-key" not in repr(factory)


def test_a_timer_start_after_close_does_nothing(ws_session: WsSession) -> None:
    connection = spare_connection(ws_session, "never.test")
    connection.close()
    connection._start_later()  # the timer fired just as close() cancelled it
    assert connection.join(TIMEOUT)
    assert ws_session.opens == 0


def test_a_new_stream_cancels_a_pending_idle_close(ws_session: WsSession) -> None:
    first = ws_session.query(TRADES, ["AAPL"]).blink()
    connection = ws_session.connection()
    ws_session.subscribed("T.AAPL")
    ws_session.session.release(first)
    ws_session.unsubscribed("T.AAPL")
    assert ws_session.on_loop(connection, lambda: connection._idle is not None)
    second = ws_session.query(TRADES, ["MSFT"]).blink()
    ws_session.subscribed("T.MSFT")
    assert ws_session.on_loop(connection, lambda: connection._idle is None)
    ws_session.session.release(second)


def test_every_message_rewrites_the_row_once_the_interval_passed(
    ws_session: WsSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.subscribed("T.AAPL")
    connection = ws_session.connection()
    written: list[int] = []
    monkeypatch.setattr(ws, "_MESSAGE_ROW_NS", 0)
    monkeypatch.setattr(connection, "_write_row", lambda: written.append(1))
    ws_session.send(trade("AAPL"))
    assert written  # the message alone rewrote the row
    ws_session.session.release(result)


def test_contracts_of_opening_streams_count_against_the_cap(
    ws_session: WsSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(ws, "OPTIONS_CAP", 2)
    first = ws_session.query(OPTIONS, ["O:SPY251219C00600000"]).blink()
    connection = ws_session.connection("wss://socket.test/options")
    with ws_session.session._core.registry_lock:
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


def test_a_verdict_while_closing_moves_nothing(
    make_ws_session: Callable[..., WsSession], monkeypatch: pytest.MonkeyPatch
) -> None:
    custom_session = make_ws_session(decide=unknown_futures)
    result = custom_session.query(FUTURES, ["ESZ5"]).blink()
    connection = custom_session.connection("wss://socket.test/futures")
    closed = iter([False, True])  # open when the verdict arrives, closed by the move
    monkeypatch.setattr(custom_session.connections, "_is_closed", lambda: next(closed))
    assert custom_session.on_loop(
        connection, lambda: custom_session.connections.reroute(connection, "x")
    )
    assert connection.join(TIMEOUT)  # the move closed it instead of handing it over
    assert len(custom_session.urls) == 2  # the successor was built, then closed unused
    del result


def test_a_verdict_joins_an_open_delayed_connection(
    make_ws_session: Callable[..., WsSession],
) -> None:
    decisions = iter(
        [
            HostDecision("socket.test", Recency.UNKNOWN, "no timeframe"),
            HostDecision("delayed.test", Recency.DELAYED, "a stream already reads it"),
        ]
    )
    custom_session = make_ws_session(decide=lambda *_: next(decisions))
    custom_session.connections._reconnect_backoff = 0.01
    result = custom_session.query(FUTURES, ["ESZ5"]).blink()
    delayed = custom_session.query(QUOTES_FUTURES, ["ESZ5"]).blink()
    moved = custom_session.connection("wss://socket.test/futures")
    existing = custom_session.connection("wss://delayed.test/futures")
    assert custom_session.on_loop(
        moved, lambda: custom_session.connections.reroute(moved, "real-time")
    )
    wait_for(lambda: existing.leases == 2)
    assert custom_session.connection("wss://delayed.test/futures") is existing
    custom_session.session.release(result)
    custom_session.session.release(delayed)


# --- Races of the connection lifecycle -------------------------------------------------------


def test_a_retired_connection_is_never_leased(
    ws_session: WsSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The registry is read again after the new connection is built: a retired connection is
    # gone from it, and the lease goes to the new one, never to None.
    first = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.subscribed("T.AAPL")
    old = ws_session.connection()
    ws_session.connections.retire(old, idle=False)
    real = ws.WsConnections._check_cap
    looks: list[ws.Connection | None] = []

    def watching(
        spec: DatasetSpec, connection: ws.Connection | None, topics: frozenset[str]
    ) -> None:
        looks.append(connection)
        real(spec, connection, topics)

    monkeypatch.setattr(ws.WsConnections, "_check_cap", staticmethod(watching))
    second = ws_session.query(TRADES, ["MSFT"]).blink()
    replacement = ws_session.connection()
    assert replacement is not old
    assert looks == [None, None]  # nothing the first time, then the new one is registered
    ws_session.subscribed("T.MSFT")
    old.close()
    assert old.join(TIMEOUT)
    ws_session.session.release(first)
    ws_session.session.release(second)


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
    ws_session.send(trade("AAPL", t=10))  # a timestamp out of bounds fails the stream
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


def test_a_move_awaits_its_streams_until_the_hand_over(
    make_ws_session: Callable[..., WsSession],
) -> None:
    custom_session = make_ws_session(decide=unknown_futures)
    custom_session.connections._reconnect_backoff = 0.01
    result = custom_session.query(FUTURES, ["ESZ5"]).blink()
    custom_session.subscribed("T.ESZ5")
    old = custom_session.connection("wss://socket.test/futures")
    held, release = threading.Event(), threading.Event()

    def hold() -> None:
        held.set()
        assert release.wait(TIMEOUT)

    assert old.post(hold)  # the old loop is busy: the hand-over waits behind it
    assert held.wait(TIMEOUT)
    decision = HostDecision("delayed.test", Recency.DELAYED, "server verdict: x")
    custom_session.connections._move(old, decision)
    successor = custom_session.connection("wss://delayed.test/futures")
    with custom_session.session._core.registry_lock:
        assert successor.awaiting == {1}
    release.set()
    wait_for(lambda: not successor.awaiting)  # adopted
    custom_session.subscribed("T.ESZ5")
    custom_session.session.release(result)


def test_a_failed_connection_is_not_moved(make_ws_session: Callable[..., WsSession]) -> None:
    custom_session = make_ws_session(decide=unknown_futures)
    result = custom_session.query(FUTURES, ["ESZ5"]).blink()
    old = custom_session.connection("wss://socket.test/futures")
    with custom_session.session._core.registry_lock:
        old.failure = dm.MassiveError("rejected the API key")
    decision = HostDecision("delayed.test", Recency.DELAYED, "server verdict: x")
    custom_session.connections._move(old, decision)
    assert old.join(TIMEOUT)  # closed, not handed over
    assert "wss://delayed.test/futures" not in custom_session.connections._connections
    del result


def test_a_move_whose_old_loop_ended_releases_the_successor(
    make_ws_session: Callable[..., WsSession],
) -> None:
    custom_session = make_ws_session(decide=unknown_futures)
    custom_session.connections._reconnect_backoff = 0.01
    result = custom_session.query(FUTURES, ["ESZ5"]).blink()
    custom_session.subscribed("T.ESZ5")
    old = custom_session.connection("wss://socket.test/futures")
    old.close()
    assert old.join(TIMEOUT)  # its loop ended: the hand-over cannot be posted
    decision = HostDecision("delayed.test", Recency.DELAYED, "server verdict: x")
    custom_session.connections._move(old, decision)
    successor = custom_session.connection("wss://delayed.test/futures")
    custom_session.on_loop(successor, lambda: None)
    with custom_session.session._core.registry_lock:
        assert successor.awaiting == set()
    custom_session.on_loop(successor, lambda: successor.release_stream(1))  # dropped, not kept
    assert custom_session.on_loop(successor, lambda: dict(successor._early)) == {}
    # A call kept for an awaited stream is forgotten once the stream is no longer awaited.
    with custom_session.session._core.registry_lock:
        successor.awaiting.add(99)
    custom_session.on_loop(successor, lambda: successor.release_stream(99))
    assert custom_session.on_loop(successor, lambda: sorted(successor._early)) == [99]
    with custom_session.session._core.registry_lock:
        successor.awaiting.discard(99)
    custom_session.on_loop(successor, successor.check_idle)
    assert custom_session.on_loop(successor, lambda: dict(successor._early)) == {}
    del result


def test_a_hand_over_applies_what_the_old_flusher_queued(
    ws_session: WsSession,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    *,
    rows_when: RowsWhen,
) -> None:
    refused = ws_session.query(TRADES, ["AAPL"]).blink()
    published = ws_session.query(TRADES, ["MSFT"]).append_only()
    ws_session.subscribed("T.AAPL", "T.MSFT")
    old = ws_session.connection()
    successor = spare_connection(ws_session, "next.test")
    successor.start()

    def queue_then_hand_over() -> None:
        old.flusher.fail([1], dm.EntitlementError("refused T.AAPL"))
        old.flusher.rows("T", [{"Ticker": "MSFT", "SipTimestamp": 1_700_000_000_000}])
        old.hand_over(successor)

    ws_session.on_loop(old, queue_then_hand_over)
    assert old.join(TIMEOUT)  # its last flush applied the failure and published the row
    rows_when(published, lambda rows: len(rows) == 1)
    states = [r["State"] for r in streams_of(ws_session, dh_to_pylist)]
    assert states == ["FAILED", "RUNNING"]
    successor.close()
    assert successor.join(TIMEOUT)
    del refused


def test_a_topic_reconnects_once_per_session(ws_session: WsSession) -> None:
    assert ws_session.connections.first_no_ack("/stocks", "T.AAPL")
    assert not ws_session.connections.first_no_ack("/stocks", "T.AAPL")
    assert ws_session.connections.first_no_ack("/options", "T.AAPL")


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


def test_a_stock_stream_makes_no_calendar(ws_session: WsSession) -> None:
    result = ws_session.query(TRADES, ["AAPL"]).blink()
    ws_session.subscribed("T.AAPL")
    assert ws_session.connections._calendar is None
    ws_session.session.release(result)


def test_one_calendar_wins_a_race_and_closes_with_the_connections(
    ws_session: WsSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    created: list[futures_calendar.FuturesCalendar] = []
    real = futures_calendar.FuturesCalendar

    def racing(
        fetch: Callable[..., list[dict[str, object]]],
        *,
        contracts_path: str,
        schedules_path: str,
        label: str,
    ) -> futures_calendar.FuturesCalendar:
        def make() -> futures_calendar.FuturesCalendar:
            return real(
                fetch, contracts_path=contracts_path, schedules_path=schedules_path, label=label
            )

        calendar = make()
        created.append(calendar)
        if len(created) == 1:  # another caller installs its calendar meanwhile
            winner = make()
            created.append(winner)
            ws_session.connections._calendar = winner
        return calendar

    monkeypatch.setattr("deephaven_massive._ingest.ws.FuturesCalendar", racing)
    found = ws_session.connections.futures_calendar()
    loser, winner = created
    assert found is winner
    assert not loser._thread.is_alive()  # the loser was closed at once
    ws_session.close()
    assert not winner._thread.is_alive()


def test_no_calendar_outlives_a_closed_transport(
    ws_session: WsSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    created: list[futures_calendar.FuturesCalendar] = []
    real = futures_calendar.FuturesCalendar

    def closing(
        fetch: Callable[..., list[dict[str, object]]],
        *,
        contracts_path: str,
        schedules_path: str,
        label: str,
    ) -> futures_calendar.FuturesCalendar:
        def make() -> futures_calendar.FuturesCalendar:
            return real(
                fetch, contracts_path=contracts_path, schedules_path=schedules_path, label=label
            )

        calendar = make()
        created.append(calendar)
        ws_session.connections.close()  # the session closes while the calendar starts
        return calendar

    monkeypatch.setattr("deephaven_massive._ingest.ws.FuturesCalendar", closing)
    with pytest.raises(dm.MassiveError, match="is closed"):
        ws_session.connections.futures_calendar()
    assert not created[0]._thread.is_alive()  # closed at once, never installed
    # Once closed, no calendar is even created.
    with pytest.raises(dm.MassiveError, match="is closed"):
        ws_session.connections.futures_calendar()
    assert len(created) == 1
