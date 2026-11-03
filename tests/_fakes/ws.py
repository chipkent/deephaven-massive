"""A fake Massive WebSocket server, for the WebSocket client's tests.

It speaks Massive's protocol on `ws://127.0.0.1:<port>/<path>` from its own thread and asyncio
loop, with `websockets.asyncio.server`: each connection gets the `connected` greeting, reads
the authentication frame, and gets a reply; after `auth_success` every frame it receives is
recorded and, when `auto_ack` is set, each subscribe frame is acknowledged as Massive's
knowledge base shows it: one "subscribed to: <topic>" status object per topic, all in one
array (or, with `combined_acks`, one object naming the comma-separated topics). Tests may also
script the answers: `refuse` maps a topic to the message of the error status that refuses it,
`silent` holds topics never answered, and each entry of `step_refusals` answers the next
subscribe frame with one error status that names no topic.

The server logs through its own logger at WARNING, so its DEBUG frame log (which would show
the client's authentication frame) never reaches a test that captures every logger.

Tests script the next connections before they happen (`reject_next`, `greet_next`, and
`auth_replies`, whose entries are a status, a `(status, message)` pair, a raw frame, or
`NoReply.TO_KEY` for a server that never answers the key; `hold_slot`, for another connection
that holds the account's one slot until the test frees it) and act on the live one (`send`,
`close`) from the test thread. Everything the server observes is kept under one lock, and
`wait_for` blocks on a condition until a predicate over it holds, so tests synchronize with the
client's thread deterministically, never by sleeping. Built by the `fake_ws_server` fixture in
`tests/conftest.py`.

`TradeFeed` drives a connection like a busy market: a thread of its own sends frames of
numbered trades as fast as the server writes them, so the soak tests can check that every
table received each trade it should exactly once. For a run of hours, `keep_frames = False`
stops the server recording each frame it receives, whose list would otherwise grow with every
subscription change; each connection's `topics` stays current either way.
"""

from __future__ import annotations

import asyncio
import enum
import json
import logging
import threading
from dataclasses import dataclass, field
from http import HTTPStatus
from typing import TYPE_CHECKING

from websockets.asyncio.server import Server, ServerConnection, serve
from websockets.datastructures import Headers
from websockets.exceptions import ConnectionClosed
from websockets.http11 import Response

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from websockets.http11 import Request

# The fake's own logger: quiet below WARNING whatever level a test sets on the root.
_LOGGER = logging.getLogger("tests.fake_massive_ws")
_LOGGER.setLevel(logging.WARNING)

_MESSAGES = {
    "connected": "Connected Successfully",
    "auth_success": "authenticated",
    "auth_failed": "authentication failed",
    "max_connections": (
        "Maximum number of websocket connections exceeded. You have reached the connection "
        "limit for your account."
    ),
}


class NoReply(enum.Enum):
    """A scripted reply that is no reply: the server reads the frame and never answers it."""

    TO_KEY = "to_key"
    """The server reads the authentication frame and stays silent until the client closes, so
    only the client's handshake timeout ends the attempt."""


def status_frame(status: str, message: str | None = None) -> str:
    """Return one status frame as Massive sends it.

    Args:
        status: The `status` value, such as `"success"` or `"error"`.
        message: The `message`; by default Massive's text for a handshake status.

    Returns:
        `[{"ev":"status","status":...,"message":...}]` as text.
    """
    text = _MESSAGES.get(status, "") if message is None else message
    return json.dumps([{"ev": "status", "status": status, "message": text}])


def _auth_reply(reply: str | tuple[str, str] | bytes) -> tuple[str | bytes, str | None]:
    """Return the frame of a scripted reply to the key, and its status.

    Args:
        reply: A status, a `(status, message)` pair, or a raw frame.

    Returns:
        The frame to send, and its status (None for a raw frame).
    """
    if isinstance(reply, bytes):
        return reply, None
    status, message = reply if isinstance(reply, tuple) else (reply, None)
    return status_frame(status, message), status


def _ack_frame(
    params: str, *, combined: bool, refuse: dict[str, str], silent: set[str]
) -> str | None:
    """Return the answer to one subscribe frame.

    Args:
        params: The frame's comma-separated topics.
        combined: One status object for all acknowledged topics, instead of one per topic.
        refuse: Topics to refuse, each with the message of its error status.
        silent: Topics never answered.

    Returns:
        The status frame, or None when no topic is answered.
    """
    topics = params.split(",")
    acked = [t for t in topics if t not in refuse and t not in silent]
    statuses = [
        {"ev": "status", "status": "error", "message": refuse[t]} for t in topics if t in refuse
    ]
    if acked:
        listed = [",".join(acked)] if combined else acked
        statuses += [
            {"ev": "status", "status": "success", "message": f"subscribed to: {t}"} for t in listed
        ]
    return json.dumps(statuses) if statuses else None


@dataclass
class Connection:
    """What the server saw of one connection.

    Attributes:
        path: The request path, such as `"/stocks"`.
        user_agent: The `User-Agent` request header.
        auth: The authentication frame, or `None` before it arrived.
        frames: The frames received after authentication, in order, while the server's
            `keep_frames` is set.
        topics: The topics subscribed now: each subscribe frame adds its topics, and each
            unsubscribe frame removes its topics.
        close_code: The close code once the connection has closed, else `None`.
    """

    path: str
    user_agent: str | None
    auth: str | None = None
    frames: list[str] = field(default_factory=list)
    topics: set[str] = field(default_factory=set)
    close_code: int | None = None


class FakeMassiveServer:
    """A scripted Massive WebSocket endpoint on the loopback interface.

    Thread-safe: test threads script and inspect it under its lock while the server thread
    serves.
    """

    def __init__(self) -> None:
        """Start serving on an ephemeral port; returns once the port is bound."""
        self._lock = threading.Condition()
        self.connections: list[Connection] = []
        # Each reply is a status, sent with Massive's message for it; a (status, message) pair;
        # bytes, sent as they are in a binary frame, as a server that broke protocol would; or
        # NoReply.TO_KEY, for a server that never answers the key.
        self.auth_replies: list[str | tuple[str, str] | bytes | NoReply] = []
        self.auto_ack = True
        self.keep_frames = True
        self.combined_acks = False
        self.close_after_refusal = True
        self.refuse: dict[str, str] = {}
        self.silent: set[str] = set()
        self.step_refusals: list[str] = []
        self._slot_held = False
        self._rejects: list[tuple[int, dict[str, str]]] = []
        self._greetings: list[str] = []
        self._live: dict[int, ServerConnection] = {}
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self._loop.run_forever, name="fake-massive-ws", daemon=True
        )
        self._thread.start()

        async def bind() -> Server:
            return await serve(
                self._handle,
                "127.0.0.1",
                0,
                process_request=self._process_request,
                logger=_LOGGER,
            )

        self._server = asyncio.run_coroutine_threadsafe(bind(), self._loop).result(10.0)
        self.port: int = self._server.sockets[0].getsockname()[1]

    def url(self, path: str = "/stocks") -> str:
        """Return the URL of `path` on this server.

        Args:
            path: The cluster path.

        Returns:
            `ws://127.0.0.1:<port><path>`.
        """
        return f"ws://127.0.0.1:{self.port}{path}"

    # --- scripting ----------------------------------------------------------------------------

    def reject_next(self, status: int, headers: dict[str, str] | None = None) -> None:
        """Answer the next upgrade request with an HTTP status instead of upgrading.

        Args:
            status: The HTTP status, such as 404 or 429.
            headers: Extra response headers, such as `{"Retry-After": "40"}`.
        """
        with self._lock:
            self._rejects.append((status, headers or {}))

    def greet_next(self, frame: str) -> None:
        """Replace the `connected` greeting of the next connection.

        Args:
            frame: The frame to greet with instead.
        """
        with self._lock:
            self._greetings.append(frame)

    # --- acting on the live connection ----------------------------------------------------------

    def send(self, frame: str) -> None:
        """Send a frame on the latest connection and wait until it is written.

        Args:
            frame: The text frame.
        """
        self._call(lambda ws: ws.send(frame))

    def close(self, code: int = 1000, reason: str = "") -> None:
        """Close the latest connection with `code` and wait for the closing handshake.

        Args:
            code: The close code; 1000 is a clean close, 1011 a server error.
            reason: The close frame's reason text; empty for none.
        """
        self._call(lambda ws: ws.close(code, reason))

    def hold_slot(self, *, held: bool) -> None:
        """Take or free the account's one connection slot, as another connection would.

        While the slot is held, every authentication that `auth_replies` does not script is
        answered `max_connections`, so a client waits for the slot for as long as the test
        needs, whatever its backoff.

        Args:
            held: True to hold the slot, False to free it.
        """
        with self._lock:
            self._slot_held = held
            self._lock.notify_all()

    def hold_reads(self, *, held: bool) -> None:
        """Stop or resume reading from the latest connection.

        While reads are held, nothing the client sends is seen, so a closing handshake it
        starts stays unanswered until reads resume.

        Args:
            held: True to stop reading, False to resume.
        """

        async def toggle(ws: ServerConnection) -> None:
            if held:
                ws.transport.pause_reading()
            else:
                ws.transport.resume_reading()

        self._call(toggle)

    def _call(self, action: Callable[[ServerConnection], Awaitable[object]]) -> None:
        """Run a coroutine against the latest live connection on the server's loop.

        Args:
            action: Builds the coroutine from the connection.
        """
        with self._lock:
            ws = self._live[len(self.connections) - 1]

        async def run() -> None:
            await action(ws)

        asyncio.run_coroutine_threadsafe(run(), self._loop).result(10.0)

    # --- observing ----------------------------------------------------------------------------

    def wait_for(
        self, predicate: Callable[[FakeMassiveServer], bool], timeout: float = 10.0
    ) -> None:
        """Block until `predicate(self)` holds, checking it whenever the server observes something.

        Args:
            predicate: Reads the server's state; called with its lock held.
            timeout: The longest wait in seconds.

        Raises:
            AssertionError: If the predicate still fails after `timeout`.
        """
        with self._lock:
            if not self._lock.wait_for(lambda: predicate(self), timeout):
                raise AssertionError(f"fake server: condition not met in {timeout} s")

    def frames(self, index: int = -1) -> list[str]:
        """Return a copy of the frames one connection received after authentication.

        Args:
            index: The connection's position; the latest by default.

        Returns:
            The frames, in order.
        """
        with self._lock:
            return list(self.connections[index].frames)

    def stop(self) -> None:
        """Stop serving, close every connection, and end the server's thread; idempotent."""
        if self._loop.is_closed():
            return

        async def shutdown() -> None:
            self._server.close()
            await self._server.wait_closed()

        asyncio.run_coroutine_threadsafe(shutdown(), self._loop).result(10.0)
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(10.0)
        self._loop.close()

    # --- the server ---------------------------------------------------------------------------

    def _process_request(self, connection: ServerConnection, request: Request) -> Response | None:
        """Refuse the upgrade when a rejection is scripted.

        Returns:
            The scripted HTTP response, or `None` to upgrade.
        """
        with self._lock:
            if not self._rejects:
                return None
            status, headers = self._rejects.pop(0)
        reason = HTTPStatus(status).phrase
        return Response(status, reason, Headers(headers), reason.encode())

    def _record(self, record: Connection, frame: str, request: dict[str, object]) -> None:
        """Note a frame the client sent; the lock is held.

        Args:
            record: The connection's record.
            frame: The frame as received.
            request: The frame decoded.
        """
        if self.keep_frames:
            record.frames.append(frame)
        named = set(str(request.get("params", "")).split(",")) - {""}
        if request.get("action") == "subscribe":
            record.topics |= named
        elif request.get("action") == "unsubscribe":
            record.topics -= named

    async def _handle(self, ws: ServerConnection) -> None:
        """Serve one connection by Massive's protocol."""
        assert ws.request is not None  # set once the upgrade succeeded
        with self._lock:
            index = len(self.connections)
            record = Connection(
                path=ws.request.path, user_agent=ws.request.headers.get("User-Agent")
            )
            self.connections.append(record)
            self._live[index] = ws
            greeting = self._greetings.pop(0) if self._greetings else status_frame("connected")
            self._lock.notify_all()
        try:
            await ws.send(greeting)
            auth = await ws.recv()
            with self._lock:
                record.auth = str(auth)
                if self.auth_replies:
                    reply = self.auth_replies.pop(0)
                else:
                    reply = "max_connections" if self._slot_held else "auth_success"
                self._lock.notify_all()
            if reply is NoReply.TO_KEY:  # the client's handshake timeout must end it
                await ws.wait_closed()
                return
            frame, status = _auth_reply(reply)
            await ws.send(frame)
            if status != "auth_success":
                if self.close_after_refusal:
                    await ws.close()
                await ws.wait_closed()
                return
            async for frame in ws:
                request = json.loads(frame)
                subscribe = request.get("action") == "subscribe"
                with self._lock:
                    self._record(record, str(frame), request)
                    ack = self.auto_ack
                    combined = self.combined_acks
                    refuse, silent = dict(self.refuse), set(self.silent)
                    step = self.step_refusals.pop(0) if subscribe and self.step_refusals else None
                    self._lock.notify_all()
                if step is not None:
                    await ws.send(status_frame("error", step))
                elif ack and subscribe:
                    answer = _ack_frame(
                        request["params"], combined=combined, refuse=refuse, silent=silent
                    )
                    if answer is not None:
                        await ws.send(answer)
        except ConnectionClosed:
            pass  # the client went away mid-protocol, as a client under test may
        finally:
            await ws.wait_closed()
            with self._lock:
                record.close_code = ws.close_code
                self._lock.notify_all()


class TradeFeed:
    """Sends numbered trades of a set of symbols to the server's latest connection, from a thread.

    Each frame holds `rows_per_frame` stocks trades (channel `T`), taking the symbols in turn,
    and each symbol's trades are numbered 1, 2, 3, ... in the sequence-number key `q`. So a table
    that received every trade of a symbol once holds exactly the numbers 1 to `sent()[symbol]`,
    and one that received a stretch of them holds a run with no gap and no repeat. `start()`
    begins sending, as fast as the server writes each frame; `stop()` ends the thread once the
    frame being written is done. A frame the server could not write (no connection, or a closed
    one) is counted in `undelivered`, and its trades are not counted as sent. Thread-safe: the
    counts are kept under a lock.

    Example:
        ::

            feed = TradeFeed(server, ["AAPL", "MSFT"])
            feed.start()
            ...
            feed.stop()
            assert feed.sent()["AAPL"] > 0
    """

    def __init__(
        self, server: FakeMassiveServer, symbols: list[str], *, rows_per_frame: int = 100
    ) -> None:
        """Prepare the feed; nothing is sent until `start()`.

        Args:
            server: The server whose latest connection the frames are sent on.
            symbols: The symbols to trade, in turn.
            rows_per_frame: Trades per frame.
        """
        self._server = server
        self._symbols = list(symbols)
        self._rows_per_frame = rows_per_frame
        self._lock = threading.Lock()
        self._sent = dict.fromkeys(self._symbols, 0)
        self._undelivered = 0
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="fake-massive-feed", daemon=True)

    def start(self) -> None:
        """Start sending."""
        self._thread.start()

    def stop(self) -> None:
        """Stop sending, and wait until the frame being written is done; idempotent.

        Fails the test if the thread has not ended within ten seconds.
        """
        self._stop.set()
        if self._thread.ident is not None:  # a feed never started has no thread to wait for
            self._thread.join(10.0)
        assert not self._thread.is_alive(), "the trade feed did not stop"

    def sent(self) -> dict[str, int]:
        """Return how many trades of each symbol have been written so far.

        Returns:
            The count by symbol, a copy.
        """
        with self._lock:
            return dict(self._sent)

    @property
    def undelivered(self) -> int:
        """Frames the server could not write: no connection, or a closed one."""
        with self._lock:
            return self._undelivered

    def _run(self) -> None:
        """Send frames until stopped."""
        turn = 0
        while not self._stop.is_set():
            with self._lock:
                numbered = dict(self._sent)
            rows = []
            counts: dict[str, int] = {}
            for _ in range(self._rows_per_frame):
                symbol = self._symbols[turn % len(self._symbols)]
                turn += 1
                counts[symbol] = counts.get(symbol, 0) + 1
                number = numbered[symbol] + counts[symbol]
                rows.append(
                    {
                        "ev": "T",
                        "sym": symbol,
                        "x": 4,
                        "i": str(number),
                        "p": 1.0,
                        "q": number,
                        "t": 1_700_000_000_000 + number,
                        "s": 100,
                        "z": 3,
                        "ds": "100",
                    }
                )
            try:
                self._server.send(json.dumps(rows))
            except (KeyError, ConnectionClosed, RuntimeError, TimeoutError):
                with self._lock:
                    self._undelivered += 1
                self._stop.wait(0.01)  # no connection to write to yet
                continue
            with self._lock:
                for symbol, count in counts.items():
                    self._sent[symbol] += count
