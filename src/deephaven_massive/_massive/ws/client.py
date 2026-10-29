"""The client for one Massive WebSocket endpoint, `wss://<host>/<cluster>`.

`WsClient` owns everything about the wire of one connection: the socket, the handshake and
authentication, sending subscription frames, decoding every received frame, reconnecting with
backoff, and closing. The library (`_ingest/ws.py`) owns every decision: which topics to subscribe,
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
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Final
from urllib.parse import urlsplit

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed, WebSocketException

from deephaven_massive._massive.net import USER_AGENT
from deephaven_massive._massive.net.errors import WireAuthError, WireError
from deephaven_massive._massive.net.http import ssl_context
from deephaven_massive._massive.net.redaction import Redactor
from deephaven_massive._massive.net.retry import SYSTEM_CLOCK
from deephaven_massive._massive.ws.backoff import Backoff
from deephaven_massive._massive.ws.protocol import (
    FrameDecoder,
    Status,
    StatusKind,
    auth_frame,
    classify_failure,
    handshake_status,
    subscription_frames,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterable, Sequence

    from websockets.asyncio.client import ClientConnection

    from deephaven_massive._massive.net.retry import Clock
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


# The logger every connection hands to websockets. websockets logs each frame it sends at
# DEBUG, and the authentication frame, which holds the API key, is short enough to be logged
# whole. A connection decides once, when it is created, whether to log frames, by asking its
# logger `isEnabledFor(DEBUG)`; this logger's own level answers no whatever the root's level,
# and its filter drops any DEBUG record even if someone lowers that level. websockets' INFO
# and higher records still propagate, under this name.
_WIRE_LOGGER = logger.getChild("wire")
_WIRE_LOGGER.setLevel(logging.INFO)
_WIRE_LOGGER.addFilter(lambda record: record.levelno > logging.DEBUG)


class WsState(StrEnum):
    """The connection states a client reports through its `on_state` callback.

    Being connected is reported by `on_open`, not by a state. `FAILED` and `CLOSED` are
    terminal, and a client reports exactly one of them, once: a `close()` that races a fatal
    failure does not add `CLOSED` after `FAILED`.
    """

    CONNECTING = "CONNECTING"
    """An attempt to connect and authenticate has started."""
    RECONNECTING = "RECONNECTING"
    """The connection closed or an attempt failed; the detail is the next attempt's time."""
    WAITING_SLOT = "WAITING_SLOT"
    """The account has no free connection: the server answered the key with
    `max_connections`, or sent a `max_connections` status before closing an authenticated
    socket. The client waits its backoff and tries again; the detail is the next attempt's
    time."""
    FAILED = "FAILED"
    """A fatal failure ended the client; the detail is the `WireError`. An internal failure (a
    callback that raised, an unexpected exception) also ends here, with the original exception
    as the `WireError`'s `__cause__`."""
    CLOSED = "CLOSED"
    """`close()` ended the client."""


@dataclass(frozen=True, kw_only=True)
class _Ended:
    """How an attempt or a generation ended, when the client will try again.

    Attributes:
        uptime: Seconds the generation stayed authenticated; `None` when the attempt never
            authenticated.
        waiting_slot: Whether the server reported `max_connections`, as the reply to the key
            or as a status before it closed an authenticated socket.
        least_wait: The least wait before the next attempt that the failure demands (an HTTP
            429's `Retry-After`); the backoff may be longer, and the client caps it at
            `max_reconnect_backoff`.
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
    block nor raise. An exception from a callback, or any other unexpected exception, ends the
    client: the socket is closed, the exception is logged at ERROR, and `FAILED` is reported
    with a `WireError` whose `__cause__` is the exception, so a broken client is never silently
    stale. An exception from that last `on_state` call is logged too.

    The creator must call `start()` exactly once, even when it calls `close()` first: the
    loop is created with the client, and only the thread closes it.

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
        clock: Clock = SYSTEM_CLOCK,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        thread_prefix: str = "massive",
    ) -> None:
        """Create the client and its loop; nothing connects until `start()`.

        Args:
            url: The endpoint, `wss://<host>/<path>` (or `ws://` against a local test server),
                such as `"wss://socket.massive.com/stocks"`.
            api_key: The Massive API key. It is sent only in the authentication frame and
                never logged; a frame that echoes it has it replaced with `***`
                (`net.redaction`) before it is decoded, so nothing built from it can quote it.
            sources: The WebSocket sources of the endpoint's cluster (keys `"<cluster>/<ev>"`),
                against which every frame is decoded.
            on_open: Called with each new generation's number; see the class documentation.
            on_status: Called with each status object received after authentication.
            on_data: Called with each channel's data of each frame.
            on_state: Called with each state change and its detail.
            reconnect_backoff: Seconds of the first wait before trying again; positive. The
                library passes its configuration's value (at least 15 s); tests pass less.
            max_reconnect_backoff: Seconds the doubling backoff stops at; at least
                `reconnect_backoff`. It also caps an HTTP 429's `Retry-After`, so every wait
                lies within `[reconnect_backoff, max_reconnect_backoff]`. The REST client
                instead gives up on a `Retry-After` above 60 s, because a caller is blocked on
                that request; this client never gives up, so honouring an unbounded
                `Retry-After` would leave its streams silent for as long as the server named,
                while a capped wait only retries sooner and meets another 429 at worst.
            clock: The time source the wire clients share (`net.retry.Clock`). Its
                `monotonic()` times each generation for the stable-uptime rule; its `time()`
                dates the next attempt and reads an HTTP-date `Retry-After`. Its blocking
                `wait` is never used: it would stall the loop, and with it every callback
                and keepalive ping, for a whole backoff. Tests pass a `FakeClock`.
            sleep: The coroutine that waits out a backoff on the loop; `close()` interrupts it
                by cancelling the task. Tests inject one that records the wait and returns at
                once.
            thread_prefix: The start of the client thread's name, which is
                `"<thread_prefix> ws <host><path>"`, such as `"massive #1 ws
                delayed.massive.com/stocks"`. The library passes its session's prefix
                (`"massive #<session>"`), so a log line's thread names the session; the default
                suits a client used alone.

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
        self._redactor = Redactor(api_key)
        self._ssl = ssl_context() if parts.scheme == "wss" else None
        self._decoder = FrameDecoder(sources, host=self._host, path=self._path)
        self._backoff = Backoff(reconnect_backoff, max_reconnect_backoff)
        self._max_wait = max_reconnect_backoff
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
            target=self._run, name=f"{thread_prefix} ws {self._host}{self._path}", daemon=True
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
        """Return whether the caller is anything but code running on the client's loop.

        It asks for the running loop rather than comparing thread ids, which the operating
        system reuses once the client's thread has ended.

        Returns:
            True when a loop-thread method must refuse the call.
        """
        try:
            return asyncio.get_running_loop() is not self._loop
        except RuntimeError:  # no loop runs on the calling thread
            return True

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
        with asyncio.Runner(loop_factory=lambda: self._loop) as runner:
            runner.run(self._main())

    async def _main(self) -> None:
        """Connect until a fatal failure or `close()`, then report the one terminal state."""
        self._task = asyncio.current_task()
        failure: WireError | None
        try:
            failure = None if self._closing.is_set() else await self._connect_until_fatal()
        except asyncio.CancelledError:
            failure = None  # only `close()` cancels the task
        except Exception as error:
            logger.exception("the WebSocket client for %s stopped on an internal error", self._url)
            failure = WireError(
                f"the WebSocket client for {self._host}{self._path} stopped on an internal "
                f"error: {error!r}",
                host=self._host,
                path=self._path,
            )
            failure.__cause__ = error
        else:
            if failure is not None:
                logger.error("the WebSocket client for %s failed: %s", self._url, failure)
        if failure is None:
            logger.info("closed the WebSocket client for %s", self._url)
        try:
            if failure is None:
                self._on_state(WsState.CLOSED, None)
            else:
                self._on_state(WsState.FAILED, failure)
        except Exception:
            logger.exception("on_state raised while the client for %s ended", self._url)

    async def _connect_until_fatal(self) -> WireError:
        """Attempt, run, and back off, forever, until a fatal failure.

        Returns:
            The fatal error.
        """
        while True:
            ended = await self._attempt()
            if isinstance(ended, WireError):
                return ended
            if ended.least_wait > self._max_wait:
                logger.warning(
                    "%s asked to wait %.1f s (Retry-After); waiting at most %.1f s",
                    self._url,
                    ended.least_wait,
                    self._max_wait,
                )
            least = min(ended.least_wait, self._max_wait)
            wait = max(self._backoff.next_wait(ended.uptime), least)
            logger.warning("reconnecting to %s in %.1f s", self._url, wait)
            state = WsState.WAITING_SLOT if ended.waiting_slot else WsState.RECONNECTING
            self._on_state(state, self._clock.time() + wait)
            await self._sleep(wait)

    async def _attempt(self) -> _Ended | WireError:
        """Open a socket, authenticate, and run one generation until the socket closes.

        Returns:
            How it ended, or the fatal error.
        """
        self._on_state(WsState.CONNECTING, None)
        logger.info("connecting to %s", self._url)
        deadline = asyncio.get_running_loop().time() + HANDSHAKE_TIMEOUT
        try:
            async with asyncio.timeout_at(deadline):
                websocket = await connect(
                    self._url,
                    ssl=self._ssl,
                    max_size=None,
                    open_timeout=None,
                    user_agent_header=USER_AGENT,
                    logger=_WIRE_LOGGER,
                )
        except (OSError, WebSocketException) as error:  # a timeout is an OSError
            verdict = classify_failure(
                error, host=self._host, path=self._path, now=self._clock.time()
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
        except (TimeoutError, ConnectionClosed) as error:
            logger.warning("the handshake with %s failed: %r", self._url, error)
            return _Ended()
        except ValueError:
            # The server spoke a protocol this client does not know: vendor drift, not a
            # network hiccup, so it is logged at ERROR, though the attempt is still retried.
            logger.exception("the handshake with %s broke protocol", self._url)
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
        greeting, _ = handshake_status(self._clean(await websocket.recv()))
        if greeting != "connected":
            raise ValueError(f"expected the 'connected' status, got {greeting!r}")
        await websocket.send(auth_frame(self._api_key))
        reply, message = handshake_status(self._clean(await websocket.recv()))
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
            The generation's end, with its uptime, and whether the server reported
            `max_connections` before closing.
        """
        self._generations += 1
        generation = _Generation(number=self._generations, outbox=asyncio.Queue())
        self._current = generation
        opened_at = self._clock.monotonic()
        logger.info("authenticated with %s (generation %d)", self._url, generation.number)
        sender = asyncio.create_task(self._send(websocket, generation.outbox))
        no_slot = False
        try:
            self._on_open(generation.number)
            async for frame in websocket:
                for event in self._decoder.decode(self._clean(frame)):
                    if isinstance(event, Status):
                        no_slot = no_slot or event.kind is StatusKind.MAX_CONNECTIONS
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
        return _Ended(uptime=self._clock.monotonic() - opened_at, waiting_slot=no_slot)

    def _clean(self, frame: str | bytes) -> str | bytes:
        """Return a received frame with the API key replaced, should the server echo it.

        Every frame passes through here before it is decoded, so no status, row, reject, error,
        or log line built from it can quote the key.

        Args:
            frame: The frame as received: text, or bytes for a binary frame.

        Returns:
            The frame itself when it does not hold the key, otherwise a copy with `***` in
            its place.
        """
        return self._redactor.received(frame)

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
