"""The futures trade events of the raw WebSocket frames a stream received, tallied.

The checks of two open Massive problems read what the futures stream sends on the wire: whether
a trade event (`T`) carries `session_end_date`, and whether it sends its price `p` as a JSON
number or as text. Neither opens a connection of its own, since a second connection on the key
takes over the user's stream: they read the frames that the streamed-tickers live test
received on its own connection, earlier in the same run, through the wire package's
reply-capture hook (`set_reply_capture`). `capturing_trade_frames` installs the hook for one
block and yields the `TradeFrames` tally; the test drains it while the stream runs and reads
its counts and examples after. The hook hands each reply to `TradeFrames.receive` on the
thread that received it, which only queues the reply, so the tallies are touched by the test
thread alone, in `drain`.
"""

from __future__ import annotations

import contextlib
import json
import queue
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, assert_never

from deephaven_massive._massive import CapturedReply, set_reply_capture

if TYPE_CHECKING:
    from collections.abc import Iterator

__all__ = ["TradeFrames", "capturing_trade_frames"]


@dataclass
class TradeFrames:
    """The futures trade events of the raw WebSocket frames a stream received, tallied.

    The stream's client hands each reply to `receive` on its own thread, which only queues it;
    the test thread tallies the queued replies in `drain`, so only it touches the tallies.
    """

    replies: queue.SimpleQueue[CapturedReply] = field(default_factory=queue.SimpleQueue)
    source: str = ""
    """The endpoint the frames came from, such as `wss://socket.massive.com/futures`."""
    trades: int = 0
    """Trade (`T`) events received."""
    dated: int = 0
    """Trade events that carry `session_end_date`."""
    dated_example: str = ""
    """The first trade event with `session_end_date`, as Massive sent it."""
    undated_example: str = ""
    """The first trade event without it, as Massive sent it."""
    text_prices: int = 0
    """Trade events whose `p` is not a JSON number (a string, or absent)."""
    text_price_example: str = ""
    """The first trade event whose `p` is not a JSON number, as Massive sent it."""
    number_price_example: str = ""
    """The first trade event whose `p` is a JSON number, as Massive sent it."""
    unreadable: int = 0
    """Frames that were not JSON, which hold no trade to judge."""

    def receive(self, reply: CapturedReply) -> None:
        """Queue one reply; runs on the thread that received it, so it only queues.

        Args:
            reply: The reply, a REST body or a WebSocket frame.
        """
        self.replies.put(reply)

    def drain(self) -> None:
        """Tally every queued WebSocket frame; a REST reply (a calendar lookup) is passed over."""
        while not self.replies.empty():  # the test thread is the only reader
            reply = self.replies.get()
            match reply.transport:
                case "ws":
                    self._tally(reply)
                case "rest":
                    pass
                case _:
                    assert_never(reply.transport)

    def _tally(self, reply: CapturedReply) -> None:
        """Count the trade events of one frame, and keep the first of each kind as sent.

        Args:
            reply: A WebSocket frame.
        """
        self.source = f"wss://{reply.host}{reply.path}"
        try:
            events = json.loads(reply.text)
        except json.JSONDecodeError:
            # Not a frame of events: counted, so a run can say how many were not judged.
            self.unreadable += 1
            return
        for event in events:
            # A frame also carries status events, which are not trades.
            if not isinstance(event, dict) or event.get("ev") != "T":
                continue
            self.trades += 1
            sent = json.dumps(event, separators=(",", ":"))
            if "session_end_date" in event:
                self.dated += 1
                self.dated_example = self.dated_example or sent
            else:
                self.undated_example = self.undated_example or sent
            price = event.get("p")  # an absent price is not a number either
            if isinstance(price, int | float) and not isinstance(price, bool):
                self.number_price_example = self.number_price_example or sent
            else:
                self.text_prices += 1
                self.text_price_example = self.text_price_example or sent


@contextlib.contextmanager
def capturing_trade_frames() -> Iterator[TradeFrames]:
    """Tally every reply the process receives in the block, and remove the hook after it.

    Installs the process's reply-capture sink for the block only, replacing any sink installed
    before, and removes it however the block ends; the replies that arrived are tallied once
    it ends, so the tally is complete when the caller reads it.

    Yields:
        The tally; call its `drain` while the block runs to keep the queue short.
    """
    frames = TradeFrames()
    set_reply_capture(frames.receive)
    try:
        yield frames
    finally:
        set_reply_capture(None)
        frames.drain()
