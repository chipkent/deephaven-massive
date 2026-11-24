"""Tests of the futures trade-frame tally (`tests/_live/futures_trade_frames.py`)."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Literal

import pytest

from deephaven_massive._massive import CapturedReply
from tests._live import futures_trade_frames
from tests._live.futures_trade_frames import TradeFrames, capturing_trade_frames

if TYPE_CHECKING:
    from collections.abc import Callable


def _reply(text: str, *, transport: Literal["rest", "ws"] = "ws") -> CapturedReply:
    host = "socket.massive.com" if transport == "ws" else "api.massive.com"
    return CapturedReply(
        transport=transport,
        host=host,
        path="/futures" if transport == "ws" else "/futures/v1/contracts",
        query=(),
        status=None if transport == "ws" else 200,
        request_id=None,
        received_at=0,
        text=text,
    )


_DATED = {"ev": "T", "sym": "ESZ6", "p": 6800.25, "s": 1, "session_end_date": "2026-11-24"}
_UNDATED_TEXT = {"ev": "T", "sym": "CLF7", "p": "61.5", "s": 2}
_STATUS = {"ev": "status", "status": "connected"}


def _tallied(*frames: object) -> TradeFrames:
    tally = TradeFrames()
    for frame in frames:
        tally.receive(_reply(frame if isinstance(frame, str) else json.dumps(frame)))
    tally.drain()
    return tally


def test_a_frame_s_trade_events_are_counted_by_date_and_price_kind() -> None:
    tally = _tallied([_STATUS, _DATED, _UNDATED_TEXT, "not an event"], [_DATED])
    assert tally.source == "wss://socket.massive.com/futures"
    assert (tally.trades, tally.dated, tally.text_prices, tally.unreadable) == (3, 2, 1, 0)
    assert tally.dated_example == json.dumps(_DATED, separators=(",", ":"))
    assert tally.undated_example == json.dumps(_UNDATED_TEXT, separators=(",", ":"))
    assert tally.number_price_example == tally.dated_example
    assert tally.text_price_example == tally.undated_example


def test_the_examples_keep_the_first_event_of_each_kind() -> None:
    second_dated = {**_DATED, "p": 6801.0}
    second_text = {**_UNDATED_TEXT, "p": "62.0"}
    tally = _tallied([_DATED, second_dated], [_UNDATED_TEXT, second_text])
    assert tally.dated_example == json.dumps(_DATED, separators=(",", ":"))
    assert tally.text_price_example == json.dumps(_UNDATED_TEXT, separators=(",", ":"))


@pytest.mark.parametrize("value", [None, "", "2026-13-01", "20261124", "Nov 24, 2026", 2026])
def test_a_session_end_date_that_is_not_a_date_counts_as_undated(value: object) -> None:
    event = {**_DATED, "session_end_date": value}
    tally = _tallied([event])
    assert (tally.trades, tally.dated, tally.dated_example) == (1, 0, "")
    assert tally.undated_example == json.dumps(event, separators=(",", ":"))


@pytest.mark.parametrize("price", [None, True, "7676.5"])
def test_a_price_that_is_not_a_json_number_counts_as_text(price: object) -> None:
    event = {"ev": "T", "sym": "ESZ6", "s": 1}
    if price is not None:
        event["p"] = price
    tally = _tallied([event])
    assert (tally.trades, tally.text_prices) == (1, 1)
    assert tally.number_price_example == ""


def test_a_frame_that_is_not_json_is_counted_as_unreadable() -> None:
    tally = _tallied("{not json", [_DATED])
    assert (tally.unreadable, tally.trades) == (1, 1)


def test_a_rest_reply_is_not_tallied() -> None:
    tally = TradeFrames()
    tally.receive(_reply(json.dumps({"results": [_DATED]}), transport="rest"))
    tally.drain()
    assert (tally.trades, tally.unreadable, tally.source) == (0, 0, "")
    assert tally.replies.empty()


def test_a_reply_of_an_unknown_transport_is_refused() -> None:
    tally = TradeFrames()
    unknown = CapturedReply(
        transport="udp",  # type: ignore[arg-type]  # the point: refused at run time
        host="socket.massive.com",
        path="/futures",
        query=(),
        status=None,
        request_id=None,
        received_at=0,
        text="[]",
    )
    tally.receive(unknown)
    with pytest.raises(AssertionError):
        tally.drain()


@pytest.fixture
def installed(monkeypatch: pytest.MonkeyPatch) -> list[Callable[[CapturedReply], None] | None]:
    """Record each sink the block installs or removes, in place of the wire package's hook.

    Returns:
        The sinks, in the order they were set: the tally's `receive`, then None.
    """
    sinks: list[Callable[[CapturedReply], None] | None] = []
    monkeypatch.setattr(futures_trade_frames, "set_reply_capture", sinks.append)
    return sinks


def test_the_block_installs_the_tally_s_sink_and_removes_it_after(
    installed: list[Callable[[CapturedReply], None] | None],
) -> None:
    with capturing_trade_frames() as frames:
        assert installed == [frames.receive]
        frames.receive(_reply(json.dumps([_DATED])))  # arrives on the client's thread
        assert frames.trades == 0  # not yet drained
    assert installed == [frames.receive, None]
    assert (frames.trades, frames.dated) == (1, 1)


def _stream_that_fails(installed: list[Callable[[CapturedReply], None] | None]) -> None:
    with capturing_trade_frames() as frames:
        frames.receive(_reply(json.dumps([_UNDATED_TEXT])))
        msg = "the stream failed"
        raise RuntimeError(msg)


def test_the_sink_is_removed_when_the_block_raises(
    installed: list[Callable[[CapturedReply], None] | None],
) -> None:
    with pytest.raises(RuntimeError, match="the stream failed"):
        _stream_that_fails(installed)
    assert len(installed) == 2
    assert installed[1] is None
    assert callable(installed[0])
