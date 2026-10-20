"""Multiplexed raw-JSON WebSocket connection pool with reference-counted subscriptions.

`WebSocketMultiplexer` shares a single underlying WebSocket connection per `ws_url`
across any number of active `FeedBuilder` instances, reference-counting individual
subscription topics (`T.AAPL`, `Q.MSFT`, etc.) and dispatching decoded JSON event
batches directly to registered `MicroBatchPublisher` instances.
"""

from __future__ import annotations

import asyncio
import json
import threading
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import TYPE_CHECKING, Any

from deephaven_massive.config import MassiveConfig, MassiveEntitlementError

if TYPE_CHECKING:
    from deephaven_massive.engine.publisher import MicroBatchPublisher

MAX_OPTION_CONTRACTS = 1000


def _default_ws_connector(ws_url: str) -> Any:
    """Open an async WebSocket connection context manager using the `websockets` package.

    Args:
        ws_url: Target `wss://` endpoint URL.

    Returns:
        An async context manager yielding a connected WebSocket client protocol.
    """
    import websockets

    return websockets.connect(ws_url)


class _EndpointState:
    """Internal per-URL state tracking reference-counted topics and channel publishers."""

    def __init__(self, ws_url: str) -> None:
        """Initialize endpoint state for `ws_url`."""
        self.ws_url = ws_url
        self.topic_ref_counts: dict[str, int] = defaultdict(int)
        self.channel_publishers: dict[str, list[MicroBatchPublisher]] = defaultdict(list)
        self.publisher_topics: dict[tuple[str, MicroBatchPublisher], set[str]] = defaultdict(set)
        self.outbox: list[dict[str, Any]] = []
        self.last_error: Exception | None = None
        self.active_ws: Any = None
        self.authenticated: bool = False
        self.connecting: bool = False
        self.conn_task: asyncio.Task[None] | None = None


class WebSocketMultiplexer:
    """Manages shared WebSocket connections per `ws_url` and dispatches raw JSON batches to publishers.

    Key responsibilities:
    - Maintains a single socket connection per `ws_url` (`wss://socket.massive.com/stocks`,
      `wss://socket.massive.com/options`, etc.).
    - Tracks reference counts per subscription topic so overlapping feeds only emit
      `"subscribe"` on `0 -> 1` transitions and `"unsubscribe"` on `1 -> 0` transitions.
    - Enforces the 1,000-contract subscription cap on OPRA options WebSocket endpoints.
    - Automatically re-authenticates and resubscribes all active topics after transient
      network disconnects.
    """

    def __init__(
        self,
        config: MassiveConfig,
        *,
        send_hook: Callable[[str, dict[str, Any]], None] | None = None,
        ws_connector: Callable[[str], Any] | None = None,
        reconnect_delay_s: float = 0.5,
    ) -> None:
        """Initialize a `WebSocketMultiplexer`.

        Args:
            config: Session `MassiveConfig` providing API credentials.
            send_hook: Optional callback `(ws_url, payload)` invoked on outbound control
                frames for deterministic testing.
            ws_connector: Optional custom async WebSocket connector callable.
            reconnect_delay_s: Delay in seconds before reconnecting after a socket error.
        """
        self.config = config
        self._send_hook = send_hook
        self._ws_connector = ws_connector
        self._reconnect_delay_s = reconnect_delay_s
        self._lock = threading.Lock()
        self._endpoints: dict[str, _EndpointState] = {}

        self._loop: asyncio.AbstractEventLoop | None = None
        self._loop_thread: threading.Thread | None = None
        self._closed = False

    def _ensure_loop(self) -> asyncio.AbstractEventLoop:
        """Lazily start the background asyncio event loop thread for lock-free scheduling."""
        with self._lock:
            if self._loop is not None and not self._loop.is_closed():
                return self._loop
            loop = asyncio.new_event_loop()
            ready = threading.Event()

            def _run() -> None:
                asyncio.set_event_loop(loop)
                ready.set()
                loop.run_forever()

            thread = threading.Thread(target=_run, name="massive-ws-event-loop", daemon=True)
            thread.start()
            ready.wait(timeout=2.0)
            self._loop = loop
            self._loop_thread = thread
            return loop

    def call_soon_threadsafe(self, callback: Callable[..., Any], *args: Any) -> None:
        """Schedule a non-blocking callback on the multiplexer's background event loop thread.

        Args:
            callback: Callable to invoke on the event loop thread.
            *args: Positional arguments forwarded to `callback`.
        """
        loop = self._ensure_loop()
        loop.call_soon_threadsafe(callback, *args)

    def _get_endpoint(self, ws_url: str) -> _EndpointState:
        """Return or create the `_EndpointState` for `ws_url`."""
        state = self._endpoints.get(ws_url)
        if state is None:
            state = _EndpointState(ws_url)
            self._endpoints[ws_url] = state
        return state

    @staticmethod
    def _schedule_send(ws_ref: Any, msg_str: str) -> None:
        """Create and schedule a WebSocket send task inside the running event loop thread."""
        asyncio.get_running_loop().create_task(ws_ref.send(msg_str))

    def _start_connection_task(self, ws_url: str) -> None:
        """Create and track the background connection task inside the running event loop thread."""
        task = asyncio.get_running_loop().create_task(self._run_connection(ws_url))
        with self._lock:
            self._get_endpoint(ws_url).conn_task = task

    @staticmethod
    def _stop_loop(
        loop: asyncio.AbstractEventLoop,
        tasks: list[asyncio.Task[None]],
    ) -> None:
        """Cancel any active connection tasks and stop the event loop on the loop thread."""
        for task in tasks:
            task.cancel()
        loop.stop()

    def _emit_action(self, state: _EndpointState, action: str, params: str) -> dict[str, Any]:
        payload = {"action": action, "params": params}
        state.outbox.append(payload)
        if self._send_hook is not None:
            self._send_hook(state.ws_url, payload)
        elif state.authenticated and state.active_ws is not None and self._loop is not None:
            ws_ref = state.active_ws
            msg_str = json.dumps(payload)
            self._loop.call_soon_threadsafe(self._schedule_send, ws_ref, msg_str)
        return payload

    def _ensure_connection(self, ws_url: str) -> None:
        """Ensure an active background WebSocket connection loop is running for `ws_url`."""
        with self._lock:
            state = self._get_endpoint(ws_url)
            if state.connecting or self._closed:
                return
            state.connecting = True

        loop = self._ensure_loop()
        loop.call_soon_threadsafe(self._start_connection_task, ws_url)

    async def _run_connection(self, ws_url: str) -> None:
        """Maintain a live WebSocket connection for `ws_url` with auth handshake and reconnect backoff."""
        connector = self._ws_connector or _default_ws_connector
        while not self._closed:
            with self._lock:
                state = self._get_endpoint(ws_url)
                has_topics = any(c > 0 for c in state.topic_ref_counts.values())
                if not has_topics:
                    state.connecting = False
                    return

            try:
                async with connector(ws_url) as ws:
                    with self._lock:
                        state = self._get_endpoint(ws_url)
                        state.active_ws = ws
                        state.authenticated = False
                    await ws.send(json.dumps({"action": "auth", "params": self.config.api_key}))
                    async for raw_msg in ws:
                        self.handle_raw_message(ws_url, raw_msg)
            except MassiveEntitlementError:
                with self._lock:
                    state = self._get_endpoint(ws_url)
                    state.active_ws = None
                    state.authenticated = False
                    state.connecting = False
                return
            except Exception as exc:
                with self._lock:
                    state = self._get_endpoint(ws_url)
                    state.last_error = exc
                    state.active_ws = None
                    state.authenticated = False
                await asyncio.sleep(self._reconnect_delay_s)

    def register_publisher(
        self,
        ws_url: str,
        channel: str,
        publisher: MicroBatchPublisher,
    ) -> None:
        """Register a `MicroBatchPublisher` to receive events for `(ws_url, channel)`.

        Args:
            ws_url: Target WebSocket endpoint URL.
            channel: WebSocket event type (`"T"`, `"Q"`, `"AM"`, etc.).
            publisher: `MicroBatchPublisher` to register.
        """
        with self._lock:
            state = self._get_endpoint(ws_url)
            pubs = state.channel_publishers[channel]
            if publisher not in pubs:
                pubs.append(publisher)

    def unregister_publisher(
        self,
        ws_url: str,
        channel: str,
        publisher: MicroBatchPublisher,
    ) -> None:
        """Remove a `MicroBatchPublisher` from `(ws_url, channel)` and release its topic references.

        Args:
            ws_url: Target WebSocket endpoint URL.
            channel: WebSocket event type.
            publisher: `MicroBatchPublisher` to unregister.
        """
        with self._lock:
            state = self._endpoints.get(ws_url)
            if state is None:
                return
            rem_topics = list(state.publisher_topics.get((channel, publisher), ()))
            pubs = state.channel_publishers.get(channel)
            if pubs is not None and publisher in pubs:
                pubs.remove(publisher)
        if rem_topics:
            self.unsubscribe(ws_url, channel, rem_topics, publisher)

    def subscribe(
        self,
        ws_url: str,
        channel: str,
        topics: Iterable[str],
        publisher: MicroBatchPublisher | None = None,
    ) -> list[str]:
        """Increment reference counts for `topics` and emit a `"subscribe"` frame for newly active topics.

        Args:
            ws_url: Target WebSocket endpoint URL.
            channel: WebSocket event channel (`"T"`, `"Q"`, `"AM"`, etc.).
            topics: Iterable of formatted topic strings (e.g., `["T.AAPL", "T.MSFT"]`).
            publisher: Optional `MicroBatchPublisher` acquiring the topic references.

        Returns:
            List of topics that transitioned from reference count `0 -> 1`.

        Raises:
            ValueError: If subscribing on an `/options` endpoint would exceed the
                1,000-contract limit (`MAX_OPTION_CONTRACTS`).
        """
        topic_list = [t.strip() for t in topics if t and t.strip()]
        if not topic_list:
            return []

        with self._lock:
            state = self._get_endpoint(ws_url)
            pub_set: set[str] | None = None
            if publisher is not None:
                pubs = state.channel_publishers[channel]
                if publisher not in pubs:
                    pubs.append(publisher)
                pub_set = state.publisher_topics[(channel, publisher)]

            # Enforce 1,000-contract limit on OPRA options WebSocket endpoints
            if ws_url.endswith("/options"):
                active_contracts = {
                    t.split(".", 1)[1]
                    for t, cnt in state.topic_ref_counts.items()
                    if cnt > 0 and "." in t and not t.endswith("*")
                }
                new_contracts = {
                    t.split(".", 1)[1] for t in topic_list if "." in t and not t.endswith("*")
                }
                total_contracts = len(active_contracts | new_contracts)
                if total_contracts > MAX_OPTION_CONTRACTS:
                    raise ValueError(
                        f"Options WebSocket connection '{ws_url}' exceeds the {MAX_OPTION_CONTRACTS}-contract "
                        f"subscription limit ({total_contracts} requested). Use a wildcard ('*') or filter contracts."
                    )

            newly_subscribed: list[str] = []
            for topic in topic_list:
                if pub_set is not None:
                    if topic in pub_set:
                        continue
                    pub_set.add(topic)
                prev = state.topic_ref_counts[topic]
                state.topic_ref_counts[topic] = prev + 1
                if prev == 0:
                    newly_subscribed.append(topic)

            if newly_subscribed:
                self._emit_action(state, "subscribe", ",".join(newly_subscribed))

        if self._send_hook is None and newly_subscribed:
            self._ensure_connection(ws_url)

        return newly_subscribed

    def unsubscribe(
        self,
        ws_url: str,
        channel: str,
        topics: Iterable[str],
        publisher: MicroBatchPublisher | None = None,
    ) -> list[str]:
        """Decrement reference counts for `topics` and emit an `"unsubscribe"` frame for topics reaching zero.

        Args:
            ws_url: Target WebSocket endpoint URL.
            channel: WebSocket event channel.
            topics: Iterable of formatted topic strings to release.
            publisher: Optional `MicroBatchPublisher` releasing the topic references.

        Returns:
            List of topics that transitioned from reference count `1 -> 0`.
        """
        topic_list = [t.strip() for t in topics if t and t.strip()]
        if not topic_list:
            return []

        with self._lock:
            state = self._endpoints.get(ws_url)
            if state is None:
                return []

            pub_set = (
                state.publisher_topics.get((channel, publisher)) if publisher is not None else None
            )

            fully_unsubscribed: list[str] = []
            for topic in topic_list:
                if publisher is not None:
                    if pub_set is None or topic not in pub_set:
                        continue
                    pub_set.remove(topic)
                prev = state.topic_ref_counts.get(topic, 0)
                if prev <= 1:
                    state.topic_ref_counts.pop(topic, None)
                    if prev == 1:
                        fully_unsubscribed.append(topic)
                else:
                    state.topic_ref_counts[topic] = prev - 1

            if publisher is not None and not pub_set:
                state.publisher_topics.pop((channel, publisher), None)
                pubs = state.channel_publishers.get(channel)
                if pubs is not None and publisher in pubs:
                    pubs.remove(publisher)

            if fully_unsubscribed:
                self._emit_action(state, "unsubscribe", ",".join(fully_unsubscribed))

            return fully_unsubscribed

    def active_topics(self, ws_url: str) -> frozenset[str]:
        """Return the set of currently active (`ref_count > 0`) topics on `ws_url`.

        Args:
            ws_url: Target WebSocket endpoint URL.

        Returns:
            Frozenset of active topic strings.
        """
        with self._lock:
            state = self._endpoints.get(ws_url)
            if state is None:
                return frozenset()
            return frozenset(t for t, c in state.topic_ref_counts.items() if c > 0)

    def endpoint_snapshots(self) -> list[dict[str, Any]]:
        """Return a diagnostic snapshot of all tracked WebSocket endpoints.

        Returns:
            List of dictionaries suitable for constructing the `MassiveSession.status()` table.
        """
        with self._lock:
            rows: list[dict[str, Any]] = []
            for ws_url, state in sorted(self._endpoints.items()):
                active = sorted(t for t, c in state.topic_ref_counts.items() if c > 0)
                rows.append(
                    {
                        "WsUrl": ws_url,
                        "Authenticated": bool(state.authenticated),
                        "Connecting": bool(state.connecting),
                        "ActiveTopicCount": len(active),
                        "ActiveTopics": ",".join(active),
                        "LastError": (
                            str(state.last_error) if state.last_error is not None else None
                        ),
                    }
                )
            return rows

    def outbox_messages(self, ws_url: str) -> list[dict[str, Any]]:
        """Return a copy of sent control messages for `ws_url`.

        Args:
            ws_url: Target WebSocket endpoint URL.

        Returns:
            List of `{"action": ..., "params": ...}` dictionaries sent to `ws_url`.
        """
        with self._lock:
            state = self._endpoints.get(ws_url)
            return list(state.outbox) if state is not None else []

    def simulate_reconnect(self, ws_url: str) -> list[dict[str, Any]]:
        """Emit `"auth"` and `"subscribe"` control frames for all active topics on `ws_url` after reconnect.

        Args:
            ws_url: Target WebSocket endpoint URL.

        Returns:
            List of control frame dictionaries emitted during the reconnect sequence.
        """
        with self._lock:
            state = self._get_endpoint(ws_url)
            sent: list[dict[str, Any]] = []
            sent.append(self._emit_action(state, "auth", self.config.api_key))
            active = sorted(t for t, c in state.topic_ref_counts.items() if c > 0)
            if active:
                sent.append(self._emit_action(state, "subscribe", ",".join(active)))
            return sent

    def handle_raw_message(
        self,
        ws_url: str,
        raw_payload: str | bytes | Sequence[Mapping[str, Any]] | Mapping[str, Any],
    ) -> int:
        """Decode a raw WebSocket JSON frame and dispatch grouped event rows to registered publishers.

        Args:
            ws_url: WebSocket endpoint URL that received the frame.
            raw_payload: Raw JSON string/bytes or pre-decoded JSON object/array.

        Returns:
            Total number of rows enqueued across all registered publishers.

        Raises:
            MassiveEntitlementError: If a `"status"` event reports an authentication or
                entitlement failure (`auth_failed`, `not_authorized`).
        """
        parsed = json.loads(raw_payload) if isinstance(raw_payload, (str, bytes)) else raw_payload

        events: list[Mapping[str, Any]]
        if isinstance(parsed, Mapping):
            events = [parsed]
        elif isinstance(parsed, Sequence):
            events = [e for e in parsed if isinstance(e, Mapping)]
        else:
            return 0

        grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
        for ev_obj in events:
            ev_type = str(ev_obj.get("ev", "")).strip()
            if not ev_type:
                continue
            if ev_type == "status":
                status_val = str(ev_obj.get("status", "")).lower()
                msg_val = str(ev_obj.get("message", ""))
                if status_val == "auth_success":
                    with self._lock:
                        state = self._get_endpoint(ws_url)
                        state.authenticated = True
                        active = sorted(t for t, c in state.topic_ref_counts.items() if c > 0)
                        if active and state.active_ws is not None and self._loop is not None:
                            ws_ref = state.active_ws
                            sub_msg = json.dumps(
                                {"action": "subscribe", "params": ",".join(active)}
                            )
                            self._loop.call_soon_threadsafe(self._schedule_send, ws_ref, sub_msg)
                elif status_val in ("auth_failed", "error", "not_authorized") and (
                    "auth" in status_val
                    or "not authorized" in msg_val.lower()
                    or "entitled" in msg_val.lower()
                ):
                    err = MassiveEntitlementError(
                        f"WebSocket entitlement error on '{ws_url}': {msg_val or status_val}"
                    )
                    with self._lock:
                        self._get_endpoint(ws_url).last_error = err
                    raise err
                continue
            grouped[ev_type].append(ev_obj)

        total_enqueued = 0
        for ev_type, rows in grouped.items():
            with self._lock:
                ep_state = self._endpoints.get(ws_url)
                pubs = (
                    list(ep_state.channel_publishers.get(ev_type, ()))
                    if ep_state is not None
                    else []
                )
            for pub in pubs:
                total_enqueued += pub.enqueue(rows)

        return total_enqueued

    def close(self) -> None:
        """Shut down all WebSocket subscriptions and stop the background event loop thread."""
        with self._lock:
            self._closed = True
            tasks = [st.conn_task for st in self._endpoints.values() if st.conn_task is not None]
            self._endpoints.clear()
            loop = self._loop
            thread = self._loop_thread
            self._loop = None
            self._loop_thread = None

        if loop is not None and not loop.is_closed():
            loop.call_soon_threadsafe(self._stop_loop, loop, tasks)
        if thread is not None and thread.is_alive():
            thread.join(timeout=2.0)
