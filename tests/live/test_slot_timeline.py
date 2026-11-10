"""Tests of the slot probe's timeline and verdict (`tests/_live/slot_timeline.py`)."""

from __future__ import annotations

import logging
from datetime import UTC, datetime

import pytest

from tests._live import slot_timeline
from tests._live.slot_timeline import Sample, Side

_CONNECTED = "CONNECTED"
_WAITING = "WAITING_SLOT"


def _sample(seconds: float, first: Side, second: Side) -> Sample:
    return Sample(seconds, first, second)


def _held(rows: int) -> Side:
    return Side(_CONNECTED, 0, 1, rows)


def _waiting() -> Side:
    return Side(_WAITING, 0, 0, 0)


def _connection(asset_class: str, transport: str, state: str) -> dict[str, object]:
    return {
        "Transport": transport,
        "AssetClass": asset_class,
        "State": state,
        "Reconnects": 2,
        "AckedTopics": 1,
    }


def test_a_side_reads_the_crypto_websocket_row_and_sums_the_streams() -> None:
    connections = [
        _connection("crypto", "REST", "IDLE"),
        _connection("stocks", "WebSocket", "FAILED"),
        _connection("crypto", "WebSocket", _CONNECTED),
    ]
    streams = [{"RowsPublished": 40}, {"RowsPublished": 2}]
    assert slot_timeline.side(connections, streams) == Side(_CONNECTED, 2, 1, 42)


def test_a_session_without_a_connection_row_reads_none() -> None:
    assert slot_timeline.side([], []) == Side("NONE", 0, 0, 0)


def test_the_first_holding_and_the_second_waiting_is_first_held() -> None:
    samples = [_sample(2.0 * n, _held(30 * n), _waiting()) for n in range(46)]
    assert slot_timeline.verdict(samples) == slot_timeline.FIRST_HELD


def test_the_first_stalling_while_the_second_waits_is_unclear() -> None:
    rows = [0, 30, 60, 60, 60, 60, 60, 60, 60, 90]
    samples = [_sample(2.0 * n, _held(r), _waiting()) for n, r in enumerate(rows)]
    assert slot_timeline.verdict(samples) == slot_timeline.UNCLEAR


def test_a_first_that_never_streams_is_unclear() -> None:
    samples = [_sample(2.0 * n, _held(5), _waiting()) for n in range(3)]
    assert slot_timeline.verdict(samples) == slot_timeline.UNCLEAR


def test_the_first_reconnecting_while_the_second_streams_is_newer_evicted_older() -> None:
    samples = [
        _sample(0.0, Side(_CONNECTED, 0, 1, 10), Side("CONNECTING", 0, 0, 0)),
        _sample(2.0, Side("RECONNECTING", 0, 1, 10), Side(_CONNECTED, 0, 1, 30)),
        _sample(4.0, Side(_CONNECTED, 1, 1, 12), Side(_WAITING, 0, 0, 30)),
    ]
    assert slot_timeline.verdict(samples) == slot_timeline.NEWER_EVICTED_OLDER


def test_both_reconnecting_twice_is_ping_pong() -> None:
    samples = [
        _sample(0.0, Side(_CONNECTED, 1, 1, 10), Side(_CONNECTED, 0, 1, 0)),
        _sample(60.0, Side(_CONNECTED, 3, 1, 50), Side(_WAITING, 2, 0, 90)),
    ]
    assert slot_timeline.verdict(samples) == slot_timeline.PING_PONG


@pytest.mark.parametrize(("first", "second"), [(1, 2), (2, 1)])
def test_one_reconnect_of_either_side_is_not_ping_pong(first: int, second: int) -> None:
    samples = [
        _sample(0.0, Side(_CONNECTED, 0, 1, 0), Side(_CONNECTED, 0, 1, 0)),
        _sample(60.0, Side(_CONNECTED, first, 1, 50), Side(_CONNECTED, second, 1, 90)),
    ]
    assert slot_timeline.verdict(samples) == slot_timeline.NEWER_EVICTED_OLDER


def test_the_second_streaming_while_the_first_never_reconnects_is_unclear() -> None:
    samples = [
        _sample(0.0, _held(0), Side(_CONNECTED, 0, 1, 0)),
        _sample(2.0, _held(30), Side(_CONNECTED, 0, 1, 30)),
    ]
    assert slot_timeline.verdict(samples) == slot_timeline.UNCLEAR


def test_no_samples_is_unclear() -> None:
    assert slot_timeline.verdict([]) == slot_timeline.UNCLEAR


def test_the_timeline_keeps_only_changes_and_the_last_sample() -> None:
    samples = [
        _sample(0.0, _held(0), Side("NONE", 0, 0, 0)),
        _sample(2.0, _held(30), Side("NONE", 0, 0, 0)),
        _sample(4.0, _held(60), Side("NONE", 0, 0, 0)),
        _sample(6.0, _held(90), _waiting()),
        _sample(8.0, _held(90), _waiting()),
        _sample(10.0, _held(90), _waiting()),
    ]
    assert slot_timeline.changes(samples) == [
        "   0.0 s  first CONNECTED r=0 acked=1 rows=0 | second NONE r=0 acked=0 rows=0",
        "   2.0 s  first CONNECTED r=0 acked=1 rows=30+ | second NONE r=0 acked=0 rows=0",
        "   6.0 s  first CONNECTED r=0 acked=1 rows=90+ | second WAITING_SLOT r=0 acked=0 rows=0",
        "   8.0 s  first CONNECTED r=0 acked=1 rows=90 | second WAITING_SLOT r=0 acked=0 rows=0",
        "  10.0 s  first CONNECTED r=0 acked=1 rows=90 | second WAITING_SLOT r=0 acked=0 rows=0",
    ]


def _record(message: str, thread: str | None) -> logging.LogRecord:
    record = logging.LogRecord("wire", logging.DEBUG, __file__, 1, message, None, None)
    record.created = datetime(2026, 11, 10, 16, 57, 53, 120_000, tzinfo=UTC).timestamp()
    record.threadName = thread
    return record


def test_wire_events_keep_connection_events_with_their_group() -> None:
    url = "wss://socket.massive.com/crypto"
    thread = "massive group #2 ws socket.massive.com/crypto"
    records = [
        _record(f"connecting to {url}", thread),
        _record(f"{url} status MAX_CONNECTIONS: no slot", thread),
        _record(f"{url} refused the connection (max_connections)", thread),
        _record(f"authenticated with {url} (generation 2)", thread),
        _record(f"the connection to {url} closed (code 1008, reason '') after 3.0 s up", None),
        _record(f"reconnecting to {url} in 1.0 s", thread),
    ]
    assert slot_timeline.wire_events(records) == [
        f"16:57:53.120Z massive group #2: {url} status MAX_CONNECTIONS: no slot",
        f"16:57:53.120Z massive group #2: {url} refused the connection (max_connections)",
        f"16:57:53.120Z massive group #2: authenticated with {url} (generation 2)",
        f"16:57:53.120Z ?: the connection to {url} closed (code 1008, reason '') after 3.0 s up",
    ]
