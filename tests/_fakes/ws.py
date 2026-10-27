"""A fake Massive WebSocket server, for the WebSocket client's tests.

It speaks Massive's protocol on `ws://127.0.0.1:<port>/<path>` from its own thread and asyncio
loop, with `websockets.asyncio.server`: each connection gets the `connected` greeting, reads
the authentication frame, and gets a reply; after `auth_success` every frame it receives is
recorded and, when `auto_ack` is set, each subscribe frame is acknowledged with
"subscribed to: <topics>", as Massive does.

Tests script the next connections before they happen (`reject_next`, `greet_next`,
`auth_replies`) and act on the live one (`send`, `close`) from the test thread. Everything the
server observes is kept under one lock, and `wait_for` blocks on a condition until a predicate
over it holds, so tests synchronize with the client's thread deterministically, never by
sleeping. Built by the `fake_ws_server` fixture in `tests/conftest.py`.
"""

from __future__ import annotations

import asyncio
import json
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

_MESSAGES = {
    "connected": "Connected Successfully",
    "auth_success": "authenticated",
    "auth_failed": "authentication failed",
    "max_connections": (
        "Maximum number of websocket connections exceeded. You have reached the connection "
        "limit for your account."
    ),
}


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


@dataclass
class Connection:
    """What the server saw of one connection.

    Attributes:
        path: The request path, such as `"/stocks"`.
        user_agent: The `User-Agent` request header.
        auth: The authentication frame, or `None` before it arrived.
        frames: The frames received after authentication, in order.
        close_code: The close code once the connection has closed, else `None`.
    """

    path: str
    user_agent: str | None
    auth: str | None = None
    frames: list[str] = field(default_factory=list)
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
        self.auth_replies: list[str] = []
        self.auto_ack = True
        self.close_after_refusal = True
        self._rejects: list[tuple[int, dict[str, str]]] = []
        self._greetings: list[str | None] = []
        self._live: dict[int, ServerConnection] = {}
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self._loop.run_forever, name="fake-massive-ws", daemon=True
        )
        self._thread.start()

        async def bind() -> Server:
            return await serve(self._handle, "127.0.0.1", 0, process_request=self._process_request)

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

    def greet_next(self, frame: str | None) -> None:
        """Replace the `connected` greeting of the next connection.

        Args:
            frame: The frame to greet with instead, or `None` to send nothing at all.
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

    def close(self, code: int = 1000) -> None:
        """Close the latest connection with `code` and wait for the closing handshake.

        Args:
            code: The close code; 1000 is a clean close, 1011 a server error.
        """
        self._call(lambda ws: ws.close(code))

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
            if greeting is None:
                await ws.wait_closed()
                return
            await ws.send(greeting)
            auth = await ws.recv()
            with self._lock:
                record.auth = str(auth)
                reply = self.auth_replies.pop(0) if self.auth_replies else "auth_success"
                self._lock.notify_all()
            await ws.send(status_frame(reply))
            if reply != "auth_success":
                if self.close_after_refusal:
                    await ws.close()
                await ws.wait_closed()
                return
            async for frame in ws:
                with self._lock:
                    record.frames.append(str(frame))
                    ack = self.auto_ack
                    self._lock.notify_all()
                request = json.loads(frame)
                if ack and request.get("action") == "subscribe":
                    await ws.send(status_frame("success", f"subscribed to: {request['params']}"))
        except ConnectionClosed:
            pass  # the client went away mid-protocol, as a client under test may
        finally:
            await ws.wait_closed()
            with self._lock:
                record.close_code = ws.close_code
                self._lock.notify_all()
