"""Hand every raw reply from Massive to an optional sink, for recording real replies.

Documentation examples and fixture tests want Massive's own words: the exact body of a REST
response, refusals included, and the exact text of each WebSocket frame. The clients decode
those into rows and statuses and keep nothing else, so this module gives a recorder one
process-wide place to receive them: `set_reply_capture(sink)` installs a function that the
REST client calls with every response body it reads in full, whatever its status, and the
WebSocket client with every frame it receives after authentication. `set_reply_capture(None)`
removes it. With no sink installed, which is the default, a reply costs one `None` check.

What the sink sees is already clean. Each client passes everything it receives through its
`Redactor` (`net.redaction`) before anything reads it, and hands on that cleaned text, so a
secret the server echoes reaches the sink as `***`. The sink never sees a request header, the
WebSocket authentication frame or the reply to it, or a body the client gave up reading (one
that broke off, or grew past the client's cap).

**Threading.** The sink runs on the thread that received the reply: a REST caller's thread or
fan-out worker, or a WebSocket client's loop thread. It must return quickly and must not block,
since it holds up that thread; a recorder puts each reply on a queue that a writer thread
drains. An exception the sink raises never reaches the client: it is logged at WARNING, with
its traceback, the first time only in the process. Installing or removing a sink is safe from
any thread; a reply already being handed on may still reach the sink just removed.

This module imports only the standard library. It is part of `net`, the layer every client
shares.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal
from urllib.parse import parse_qsl, urlsplit

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = ["CapturedReply", "capture_frame", "capture_response", "set_reply_capture"]

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class CapturedReply:
    """One reply from Massive, as a client received it and cleaned it of its secrets.

    Created by the REST and WebSocket clients for each reply while a sink is installed, and
    handed to that sink. Immutable, so any thread may keep and read it.

    Example:
        ::

            replies: list[CapturedReply] = []
            set_reply_capture(replies.append)
            try:
                client.get(source(Transport.REST, "/v1/marketstatus/now"), {}, {}, bucket="other")
            finally:
                set_reply_capture(None)
            assert replies[0].status == 200 and replies[0].path == "/v1/marketstatus/now"

    Attributes:
        transport: `"rest"` for a REST response, `"ws"` for a WebSocket frame.
        host: The host name the reply came from, such as `"api.massive.com"`.
        path: The request's path, such as `"/v3/trades/AAPL"`, or the WebSocket endpoint's
            path, such as `"/stocks"`.
        query: The request's query parameters as `(name, value)` pairs, in the order sent; a
            parameter repeated is repeated here. Empty for a frame. The clients never put a
            credential in a query, so none can appear here.
        status: The HTTP status, or None for a frame.
        request_id: Massive's `request_id` from a REST body's envelope, or None when the body
            has none (a frame, a proxy's page, a body that is not JSON).
        received_at: When the client finished reading the reply, in nanoseconds since the
            epoch.
        text: The body or frame, cleaned of the client's secrets, decoded as UTF-8 with every
            invalid byte replaced by U+FFFD.
    """

    transport: Literal["rest", "ws"]
    host: str
    path: str
    query: tuple[tuple[str, str], ...]
    status: int | None
    request_id: str | None
    received_at: int
    text: str


class _Hook:
    """The process's one sink, and whether a failure of it was logged yet.

    Both attributes are written only under `_lock`. `sink` is read without it, since reading
    one attribute is a single reference load, safe on every build, the free-threaded one too;
    `warned` is read under the lock as well.
    """

    __slots__ = ("sink", "warned")

    def __init__(self) -> None:
        """Start with no sink and nothing logged."""
        self.sink: Callable[[CapturedReply], None] | None = None
        self.warned = False


_hook = _Hook()
"""The installed sink, shared by every client of the process."""

_lock = threading.Lock()
"""Guards `_hook`'s writes; a leaf lock, held for an assignment or a test only."""


def set_reply_capture(sink: Callable[[CapturedReply], None] | None) -> None:
    """Install the function that receives every reply, or remove it with None.

    Call it to record Massive's replies, such as when capturing real examples for the
    documentation; remove the sink when the recording ends. One sink serves the whole process,
    every client of every session, so a second call replaces the first sink.

    Args:
        sink: Called once per reply with its `CapturedReply`, on the thread that received it;
            it must not block. None removes the installed sink.
    """
    with _lock:
        _hook.sink = sink


def capture_response(
    *, host: str, target: str, status: int, request_id: str | None, body: str | bytes | bytearray
) -> None:
    """Hand one REST response to the sink, if one is installed.

    The REST client calls it with each body it read in full, after cleaning it.

    Args:
        host: The REST host name.
        target: The request's path and query, such as `"/v3/trades/AAPL?limit=50000"`.
        status: The HTTP status.
        request_id: The envelope's `request_id`, or None.
        body: The cleaned body.
    """
    sink = _hook.sink
    if sink is None:
        return
    parts = urlsplit(target)
    reply = CapturedReply(
        transport="rest",
        host=host,
        path=parts.path,
        query=tuple(parse_qsl(parts.query, keep_blank_values=True)),
        status=status,
        request_id=request_id,
        received_at=time.time_ns(),
        text=_text(body),
    )
    _deliver(sink, reply)


def capture_frame(*, host: str, path: str, frame: str | bytes) -> None:
    """Hand one WebSocket frame to the sink, if one is installed.

    The WebSocket client calls it with each frame received after authentication, after
    cleaning it.

    Args:
        host: The WebSocket host name.
        path: The endpoint's path, such as `"/stocks"`.
        frame: The cleaned frame: text, or bytes for a binary frame.
    """
    sink = _hook.sink
    if sink is None:
        return
    reply = CapturedReply(
        transport="ws",
        host=host,
        path=path,
        query=(),
        status=None,
        request_id=None,
        received_at=time.time_ns(),
        text=_text(frame),
    )
    _deliver(sink, reply)


def _text(data: str | bytes | bytearray) -> str:
    """Return a reply as text.

    Args:
        data: The cleaned reply.

    Returns:
        `data` itself when it is text, else its bytes decoded as UTF-8, each invalid byte
        replaced by U+FFFD.
    """
    if isinstance(data, str):
        return data
    return bytes(data).decode("utf-8", errors="replace")


def _deliver(sink: Callable[[CapturedReply], None], reply: CapturedReply) -> None:
    """Call the sink, keeping any exception it raises from the client.

    Args:
        sink: The installed sink.
        reply: The reply to hand on.
    """
    try:
        sink(reply)
    except Exception:  # the sink's own defect, never the client's failure
        if _first_failure():
            logger.warning(
                "The reply-capture sink raised, and the library ignores this and its later "
                "failures.",
                exc_info=True,
            )


def _first_failure() -> bool:
    """Record that the sink failed, and tell whether it is the first failure in the process.

    Only the first is logged, so a broken sink cannot flood the log with one traceback per
    reply.

    Returns:
        True the first time only.
    """
    with _lock:
        first, _hook.warned = not _hook.warned, True
    return first
