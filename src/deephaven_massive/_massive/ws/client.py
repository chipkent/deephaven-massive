"""The client for one Massive WebSocket endpoint, `wss://<host>/<cluster>`.

`WsClient` owns everything about the wire of one connection: the socket, the handshake and
authentication, sending subscription frames, decoding every received frame, reconnecting with
backoff, and closing. The library (`_ws.py`) owns every decision: which topics to subscribe,
acknowledgements and their watchdogs, routing rows to streams, and diagnostics. The two meet
through four callbacks and a handful of loop-thread methods.

**Threading.** Each client runs an asyncio loop on its own daemon thread. All its state is
confined to that thread, so it needs no lock and is correct on free-threaded Python. Only
`start()` (once, by the creator), `close()`, and `join()` may be called from another thread;
every other method, and every callback, runs on the loop thread. Another thread reaches the
loop thread with `client.loop.call_soon_threadsafe(...)`, passing plain data.

**Lifecycle.** `start()` connects at once. An attempt opens the socket, reads Massive's
`connected` greeting, sends the key, and reads the reply, all within `HANDSHAKE_TIMEOUT`. On
`auth_success` a new generation opens (`on_open`), and frames are decoded until the socket
closes. After any close or failure that `close()` did not request and that is not fatal (a
server's clean close included) the client waits its backoff (`ws.backoff`) and tries again,
forever. A fatal failure (a rejected key, a TLS certificate failure, an HTTP status that
retrying cannot fix) ends it with `FAILED`. `close()` ends it with `CLOSED` at once, whatever it
is waiting for.

This module imports the standard library, `websockets`, `net`, and `spec` (through
`ws.protocol`).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import threading
import time
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Final
from urllib.parse import urlsplit

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed, WebSocketException

from deephaven_massive._massive.net import USER_AGENT
from deephaven_massive._massive.net.errors import WireAuthError, WireError
from deephaven_massive._massive.net.http import ssl_context
from deephaven_massive._massive.ws.backoff import Backoff
from deephaven_massive._massive.ws.protocol import (
    FrameDecoder,
    Status,
    auth_frame,
    classify_failure,
    handshake_status,
    subscription_frames,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterable, Sequence

    from websockets.asyncio.client import ClientConnection

    from deephaven_massive._massive.spec.types import DriftReport, Source
    from deephaven_massive._massive.ws.protocol import Reject

__all__ = ["HANDSHAKE_TIMEOUT", "WsClient", "WsState"]

logger = logging.getLogger(__name__)

HANDSHAKE_TIMEOUT = 10.0
"""Seconds one attempt may take from opening the socket to Massive's reply to the key.

It covers TCP, TLS, any proxy, the HTTP upgrade, the `connected` greeting, and the
authentication reply, so a server that goes silent before `auth_success` cannot stall a
connection. After a `max_connections` reply it also bounds the wait for the server to close.
"""


class WsState(StrEnum):
    """The connection states a client reports through its `on_state` callback.

    Being connected is reported by `on_open`, not by a state.
    """

    CONNECTING = "CONNECTING"
    """An attempt to connect and authenticate has started."""
    RECONNECTING = "RECONNECTING"
    """The connection closed or an attempt failed; the detail is the next attempt's time."""
    WAITING_SLOT = "WAITING_SLOT"
    """The server answered the key with `max_connections`: the account has no free connection.
    The detail is the next attempt's time."""
    FAILED = "FAILED"
    """A fatal failure ended the client; the detail is the `WireError`."""
    CLOSED = "CLOSED"
    """`close()` ended the client."""


@dataclass(frozen=True, kw_only=True)
class _Ended:
    """How an attempt or a generation ended, when the client will try again.

    Attributes:
        uptime: Seconds the generation stayed authenticated; `None` when the attempt never
            authenticated.
        waiting_slot: Whether the server answered the key with `max_connections`.
        least_wait: The least wait before the next attempt that the failure demands (an HTTP
            429's `Retry-After`); the backoff may be longer.
    """

    uptime: float | None = None
    waiting_slot: bool = False
    least_wait: float = 0.0


@dataclass(frozen=True, kw_only=True)
class _Generation:
    """One authenticated socket.

    Attributes:
        number: The generation, counting from 1 per client.
        outbox: The frames to send, in order; `None` asks the sender to close the socket.
    """

    number: int
    outbox: asyncio.Queue[str | None]


# The message of a loop-thread method called from another thread.
_OFF_LOOP = "WsClient.{method} must be called on the client's loop thread"

# The server answered the key with `max_connections`.
_NO_SLOT: Final = "max_connections"


class WsClient:
    """The client of one Massive WebSocket endpoint, on its own thread.

    The library creates one per connection, passes the callbacks that receive everything the
    client learns, and calls `start()`. All state is confined to the client's loop thread;
    see the module documentation for which methods other threads may call.

    The callbacks run on the loop thread, in the order the events happen, and must neither
    block nor raise: an exception from a callback stops the client (the socket is closed and the
    exception logged at ERROR) without a state report.

    - `on_open(generation)`: Massive accepted the key on a new socket. Generations count from
      1. Subscriptions made before it are gone: the library sends its whole set again.
    - `on_status(generation, status)`: one decoded status object.
    - `on_data(generation, channel, rows, rejects, drift)`: one channel's objects of one frame.
      `rows` map every column of the channel's source (long-form names) to its value, `None`
      when absent; symbols are as the wire spells them. `rejects` are the rows that failed
      validation; `drift` is the `DriftReport` of the channel's objects in the frame.
    - `on_state(state, detail)`: the detail is the next attempt's time in seconds since the
      Unix epoch for `RECONNECTING` and `WAITING_SLOT`, the `WireError` for `FAILED`, and
      `None` for `CONNECTING` and `CLOSED`.

    Example:
        ::

            from deephaven_massive._massive import WsClient

            client = WsClient(
                "wss://socket.massive.com/stocks",
                api_key=key,
                sources=stocks_sources,
                on_open=lambda generation: client.subscribe(generation, ["T.AAPL"]),
                on_status=on_status,
                on_data=on_data,
                on_state=on_state,
                reconnect_backoff=30.0,
                max_reconnect_backoff=120.0,
            )
            client.start()
            ...
            client.close()
            client.join(5.0)
    """

    def __init__(
        self,
        url: str,
        *,
        api_key: str,
        sources: Iterable[Source],
        on_open: Callable[[int], None],
        on_status: Callable[[int, Status], None],
        on_data: Callable[[int, str, list[dict[str, object]], list[Reject], DriftReport], None],
        on_state: Callable[[WsState, float | WireError | None], None],
        reconnect_backoff: float,
        max_reconnect_backoff: float,
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        """Create the client and its loop; nothing connects until `start()`.

        Args:
            url: The endpoint, `wss://<host>/<path>` (or `ws://` against a local test server),
                such as `"wss://socket.massive.com/stocks"`.
            api_key: The Massive API key. It is sent only in the authentication frame and
                never logged.
            sources: The WebSocket sources of the endpoint's cluster (keys `"<cluster>/<ev>"`),
                against which every frame is decoded.
            on_open: Called with each new generation's number; see the class documentation.
            on_status: Called with each status object received after authentication.
            on_data: Called with each channel's data of each frame.
            on_state: Called with each state change and its detail.
            reconnect_backoff: Seconds of the first wait before trying again; positive. The
                library passes its configuration's value (at least 15 s); tests pass less.
            max_reconnect_backoff: Seconds the doubling backoff stops at; at least
                `reconnect_backoff`.
            clock: The current time in seconds since the Unix epoch. It times the
                generations for the stable-uptime rule, dates the next attempt, and reads an
                HTTP-date `Retry-After`. Tests inject a controllable clock.
            sleep: The coroutine that waits out a backoff. Tests inject one that records the
                wait and returns at once.

        Raises:
            ValueError: If `url` is not a `ws://` or `wss://` URL with a host, a source is not a
                WebSocket source or two share a channel, or the backoffs are out of range.
        """
        parts = urlsplit(url)
        if parts.scheme not in {"ws", "wss"} or not parts.hostname:
            raise ValueError(f"not a WebSocket URL: {url!r}")
        self._url = url
        self._host = parts.hostname
        self._path = parts.path or "/"
        self._api_key = api_key
        self._ssl = ssl_context() if parts.scheme == "wss" else None
        self._decoder = FrameDecoder(sources, host=self._host, path=self._path)
        self._backoff = Backoff(reconnect_backoff, max_reconnect_backoff)
        self._on_open = on_open
        self._on_status = on_status
        self._on_data = on_data
        self._on_state = on_state
        self._clock = clock
        self._sleep = sleep
        # The one piece of state other threads touch.
        self._closing = threading.Event()
        # Loop-confined from here on.
        self._task: asyncio.Task[None] | None = None
        self._cancelled = False
        self._generations = 0
        self._current: _Generation | None = None
        self._thread = threading.Thread(
            target=self._run, name=f"massive-ws {self._host}{self._path}", daemon=True
        )
        self._loop = asyncio.new_event_loop()

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        """The client's event loop, for `loop.call_soon_threadsafe` from other threads."""
        return self._loop

    def start(self) -> None:
        """Start the client's thread, which connects at once.

        Call it exactly once; a second call raises `RuntimeError` (from `threading.Thread`).
        """
        self._thread.start()

    def close(self) -> None:
        """Ask the client to stop, from any thread, without waiting.

        It sets the close request and schedules the cancellation of the client's work on its
        loop, which interrupts at once whatever it awaits: a backoff wait, a handshake, or a
        receive. The socket, if open, is then closed with code 1000 and `CLOSED` reported.
        Safe to call any number of times, before `start()` or after the client has ended
        (then it does nothing more). Use `join` to wait for the thread.
        """
        self._closing.set()
        # A closed loop raises RuntimeError: the client has ended and there is nothing to stop.
        with contextlib.suppress(RuntimeError):
            self._loop.call_soon_threadsafe(self._cancel)

    def join(self, timeout: float) -> bool:
        """Wait, from any thread but the loop's, for the client's thread to end.

        Args:
            timeout: The longest wait in seconds.

        Returns:
            Whether the thread has ended (true also for a client never started).
        """
        if self._thread.ident is not None:
            self._thread.join(timeout)
        return not self._thread.is_alive()

    def set_required(
        self, channel: str, keys: frozenset[str], *, symbol: str | None = None
    ) -> None:
        """Declare a channel's required key columns and its symbol column. Loop thread only.

        Call it before first subscribing the channel. The choice applies to every later frame,
        across reconnections, until replaced. A row missing a required key, or holding null
        there, becomes a `Reject`; any other absent field becomes `None`. A channel never
        declared has no required keys and no symbol. A channel with no source, or a column
        not in it, raises `ValueError`.

        Args:
            channel: The channel, such as `"T"`.
            keys: Long-form columns of the channel's source.
            symbol: The long-form column holding the symbol, whose raw value each `Reject`
                carries; `None` when the channel's rows carry none.

        Raises:
            RuntimeError: If called from another thread than the client's loop thread.
        """
        if self._off_loop_thread():
            raise RuntimeError(_OFF_LOOP.format(method="set_required"))
        self._decoder.set_required(channel, keys, symbol=symbol)

    def subscribe(self, generation: int, topics: Sequence[str]) -> None:
        """Send subscribe frames for `topics` at once, in call order. Loop thread only.

        The frames (at most 1,000 topics each) go on the generation's outbox, which a sender
        coroutine drains as soon as the loop runs it; nothing waits for a frame to arrive. A
        call for a generation that is no longer current is discarded, since the next
        generation starts with no subscriptions.

        Args:
            generation: The generation the topics are meant for, from `on_open`.
            topics: Exact topics such as `"T.AAPL"` or `"T.*"`; empty sends nothing.

        Raises:
            RuntimeError: If called from another thread than the client's loop thread.
        """
        if self._off_loop_thread():
            raise RuntimeError(_OFF_LOOP.format(method="subscribe"))
        self._enqueue(generation, subscription_frames("subscribe", topics))

    def unsubscribe(self, generation: int, topics: Sequence[str]) -> None:
        """Send unsubscribe frames for `topics` at once, in call order. Loop thread only.

        It behaves exactly as `subscribe` does, with the `unsubscribe` action.

        Args:
            generation: The generation the topics were subscribed in.
            topics: Exact topics; empty sends nothing.

        Raises:
            RuntimeError: If called from another thread than the client's loop thread.
        """
        if self._off_loop_thread():
            raise RuntimeError(_OFF_LOOP.format(method="unsubscribe"))
        self._enqueue(generation, subscription_frames("unsubscribe", topics))

    def reconnect(self) -> None:
        """Close the current socket, after the frames already queued, so the client reconnects.

        The close counts as a failure, so the backoff rules apply. Without an open generation
        (connecting, or waiting to reconnect) it does nothing. Loop thread only.

        Raises:
            RuntimeError: If called from another thread than the client's loop thread.
        """
        if self._off_loop_thread():
            raise RuntimeError(_OFF_LOOP.format(method="reconnect"))
        if self._current is not None:
            self._current.outbox.put_nowait(None)

    def _off_loop_thread(self) -> bool:
        """Return whether the caller is any thread but the client's loop thread.

        Returns:
            True when a loop-thread method must refuse the call.
        """
        return threading.get_ident() != self._thread.ident

    def _enqueue(self, generation: int, frames: list[str]) -> None:
        """Queue frames for the current generation, or drop them for an ended one.

        Args:
            generation: The generation the frames are meant for.
            frames: The text frames, in order.
        """
        current = self._current
        if current is None or current.number != generation:
            logger.debug("dropped %d frames for ended generation %d", len(frames), generation)
            return
        for frame in frames:
            current.outbox.put_nowait(frame)

    def _cancel(self) -> None:
        """Cancel the client's task once, on the loop thread, after `close()`.

        Before the task has started it does nothing; the task then sees the close request
        itself. Later calls do nothing, so a second `close()` cannot interrupt the closing of
        the socket.
        """
        if self._task is not None and not self._cancelled:
            self._cancelled = True
            self._task.cancel()

    def _run(self) -> None:
        """Run the client's loop until the client ends; the thread's target."""
        try:
            with asyncio.Runner(loop_factory=lambda: self._loop) as runner:
                runner.run(self._main())
        except Exception:
            logger.exception("the WebSocket client for %s stopped on an error", self._url)

    async def _main(self) -> None:
        """Connect until a fatal failure or `close()`, and report `CLOSED` after `close()`."""
        self._task = asyncio.current_task()
        try:
            if not self._closing.is_set():
                await self._connect_until_fatal()
        except asyncio.CancelledError:
            # Only `close()` cancels the task; ending normally lets the loop shut down.
            pass
        finally:
            if self._closing.is_set():
                logger.info("closed the WebSocket client for %s", self._url)
                self._on_state(WsState.CLOSED, None)

    async def _connect_until_fatal(self) -> None:
        """Attempt, run, and back off, forever, until a fatal failure."""
        while True:
            ended = await self._attempt()
            if isinstance(ended, WireError):
                logger.error("the WebSocket client for %s failed: %s", self._url, ended)
                self._on_state(WsState.FAILED, ended)
                return
            wait = max(self._backoff.next_wait(ended.uptime), ended.least_wait)
            logger.warning("reconnecting to %s in %.1f s", self._url, wait)
            state = WsState.WAITING_SLOT if ended.waiting_slot else WsState.RECONNECTING
            self._on_state(state, self._clock() + wait)
            await self._sleep(wait)

    async def _attempt(self) -> _Ended | WireError:
        """Open a socket, authenticate, and run one generation until the socket closes.

        Returns:
            How it ended, or the fatal error.
        """
        self._on_state(WsState.CONNECTING, None)
        logger.info("connecting to %s", self._url)
        deadline = asyncio.get_running_loop().time() + HANDSHAKE_TIMEOUT
        connector = connect(
            self._url,
            ssl=self._ssl,
            max_size=None,
            open_timeout=None,
            user_agent_header=USER_AGENT,
        )
        try:
            async with asyncio.timeout_at(deadline):
                websocket = await connector
        except (OSError, WebSocketException) as error:  # a timeout is an OSError
            verdict = classify_failure(
                error,
                retryable=connector.process_exception(error) is None,
                host=self._host,
                path=self._path,
                now=self._clock(),
            )
            if isinstance(verdict, WireError):
                return verdict
            logger.info("could not connect to %s: %r", self._url, error)
            return _Ended(least_wait=verdict)
        try:
            return await self._authenticate_and_run(websocket, deadline)
        finally:
            await websocket.close()

    async def _authenticate_and_run(
        self, websocket: ClientConnection, deadline: float
    ) -> _Ended | WireError:
        """Finish the handshake by `deadline`, then run the generation it opens.

        Args:
            websocket: The open socket.
            deadline: The loop time by which the server must have answered the key.

        Returns:
            How the attempt or the generation ended, or the fatal error of a rejected key.
        """
        try:
            async with asyncio.timeout_at(deadline):
                reply = await self._handshake(websocket)
        except (TimeoutError, ConnectionClosed, ValueError) as error:
            logger.warning("the handshake with %s failed: %r", self._url, error)
            return _Ended()
        if isinstance(reply, WireError):
            return reply
        if reply == _NO_SLOT:
            logger.warning(
                "%s refused the connection: the account has no free connection (max_connections)",
                self._url,
            )
            with contextlib.suppress(TimeoutError):
                async with asyncio.timeout_at(deadline):
                    await websocket.wait_closed()
            return _Ended(waiting_slot=True)
        return await self._run_generation(websocket)

    async def _handshake(self, websocket: ClientConnection) -> WireError | str | None:
        """Read the greeting, send the key, and read the reply.

        Args:
            websocket: The open socket.

        Returns:
            `None` on `auth_success`, `_NO_SLOT` on `max_connections`, and a `WireAuthError`
            on `auth_failed`.

        Raises:
            ValueError: If the greeting is not `connected` or the reply is none of those.
        """
        greeting, _ = handshake_status(await websocket.recv())
        if greeting != "connected":
            raise ValueError(f"expected the 'connected' status, got {greeting!r}")
        await websocket.send(auth_frame(self._api_key))
        reply, message = handshake_status(await websocket.recv())
        if reply == "auth_success":
            return None
        if reply == "auth_failed":
            return WireAuthError(
                f"Massive rejected the API key at {self._host}{self._path}: {message}",
                host=self._host,
                path=self._path,
            )
        if reply == _NO_SLOT:
            return _NO_SLOT
        raise ValueError(f"unexpected reply to the key: {reply!r} ({message!r})")

    async def _run_generation(self, websocket: ClientConnection) -> _Ended:
        """Open a generation and decode its frames until the socket closes.

        Args:
            websocket: The authenticated socket.

        Returns:
            The generation's end, with its uptime.
        """
        self._generations += 1
        generation = _Generation(number=self._generations, outbox=asyncio.Queue())
        self._current = generation
        opened_at = self._clock()
        logger.info("authenticated with %s (generation %d)", self._url, generation.number)
        sender = asyncio.create_task(self._send(websocket, generation.outbox))
        try:
            self._on_open(generation.number)
            async for frame in websocket:
                for event in self._decoder.decode(frame):
                    if isinstance(event, Status):
                        self._on_status(generation.number, event)
                    else:
                        self._on_data(
                            generation.number, event.channel, event.rows, event.rejects, event.drift
                        )
        except ConnectionClosed:
            pass  # an abnormal close; a clean one ends the iteration
        finally:
            self._current = None
            sender.cancel()
        logger.info("the connection to %s closed (code %s)", self._url, websocket.close_code)
        return _Ended(uptime=self._clock() - opened_at)

    @staticmethod
    async def _send(websocket: ClientConnection, outbox: asyncio.Queue[str | None]) -> None:
        """Send a generation's frames in order until the outbox asks to close or the socket closes.

        Args:
            websocket: The generation's socket.
            outbox: The frames; `None` closes the socket with code 1000.
        """
        # A closed socket ends the generation; the receiver reports it, so the sender just stops.
        with contextlib.suppress(ConnectionClosed):
            while (frame := await outbox.get()) is not None:
                logger.debug("sending %s", frame)
                await websocket.send(frame)
            await websocket.close()
