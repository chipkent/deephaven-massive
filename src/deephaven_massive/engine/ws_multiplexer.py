"""Multiplexed raw-JSON WebSocket connection pool with reference-counted subscriptions."""

from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
import json
import threading
from typing import TYPE_CHECKING, Any

from deephaven_massive.config import MassiveConfig, MassiveEntitlementError

if TYPE_CHECKING:
    from deephaven_massive.engine.publisher import MicroBatchPublisher

MAX_OPTION_CONTRACTS = 1000


class _EndpointState:
    """Internal per-URL state tracking ref-counted topics and channel publishers."""

    def __init__(self, ws_url: str) -> None:
        self.ws_url = ws_url
        self.topic_ref_counts: dict[str, int] = defaultdict(int)
        self.channel_publishers: dict[str, list[MicroBatchPublisher]] = defaultdict(list)
        self.outbox: list[dict[str, Any]] = []
        self.last_error: Exception | None = None


class WebSocketMultiplexer:
    """Manages shared WebSocket connections per `ws_url` and dispatches raw JSON batches to publishers."""

    def __init__(
        self,
        config: MassiveConfig,
        *,
        send_hook: Callable[[str, dict[str, Any]], None] | None = None,
    ) -> None:
        self.config = config
        self._send_hook = send_hook
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

            thread = threading.Thread(
                target=_run, name="massive-ws-event-loop", daemon=True
            )
            thread.start()
            ready.wait(timeout=2.0)
            self._loop = loop
            self._loop_thread = thread
            return loop

    def call_soon_threadsafe(
        self, callback: Callable[..., Any], *args: Any
    ) -> None:
        """Schedule a non-blocking callback on the multiplexer's background event loop thread."""
        loop = self._ensure_loop()
        loop.call_soon_threadsafe(callback, *args)

    def _get_endpoint(self, ws_url: str) -> _EndpointState:
        state = self._endpoints.get(ws_url)
        if state is None:
            state = _EndpointState(ws_url)
            self._endpoints[ws_url] = state
        return state

    def _emit_action(
        self, state: _EndpointState, action: str, params: str
    ) -> dict[str, Any]:
        payload = {"action": action, "params": params}
        state.outbox.append(payload)
        if self._send_hook is not None:
            self._send_hook(state.ws_url, payload)
        return payload

    def register_publisher(
        self,
        ws_url: str,
        channel: str,
        publisher: MicroBatchPublisher,
    ) -> None:
        """Register a :class:`MicroBatchPublisher` to receive events for `(ws_url, channel)`."""
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
        """Remove a :class:`MicroBatchPublisher` from `(ws_url, channel)`."""
        with self._lock:
            state = self._endpoints.get(ws_url)
            if state is not None and channel in state.channel_publishers:
                pubs = state.channel_publishers[channel]
                if publisher in pubs:
                    pubs.remove(publisher)

    def subscribe(
        self,
        ws_url: str,
        channel: str,
        topics: Iterable[str],
        publisher: MicroBatchPublisher | None = None,
    ) -> list[str]:
        """Increment reference counts for `topics` and send a subscribe frame for newly active topics."""
        topic_list = [t.strip() for t in topics if t and t.strip()]
        if not topic_list:
            return []

        with self._lock:
            state = self._get_endpoint(ws_url)
            if publisher is not None:
                pubs = state.channel_publishers[channel]
                if publisher not in pubs:
                    pubs.append(publisher)

            # Enforce 1,000-contract limit on OPRA options WebSocket endpoints
            if ws_url.endswith("/options"):
                active_contracts = {
                    t.split(".", 1)[1]
                    for t, cnt in state.topic_ref_counts.items()
                    if cnt > 0 and "." in t and not t.endswith("*")
                }
                new_contracts = {
                    t.split(".", 1)[1]
                    for t in topic_list
                    if "." in t and not t.endswith("*")
                }
                total_contracts = len(active_contracts | new_contracts)
                if total_contracts > MAX_OPTION_CONTRACTS:
                    raise ValueError(
                        f"Options WebSocket connection '{ws_url}' exceeds the {MAX_OPTION_CONTRACTS}-contract "
                        f"subscription limit ({total_contracts} requested). Use a wildcard ('*') or filter contracts."
                    )

            newly_subscribed: list[str] = []
            for topic in topic_list:
                prev = state.topic_ref_counts[topic]
                state.topic_ref_counts[topic] = prev + 1
                if prev == 0:
                    newly_subscribed.append(topic)

            if newly_subscribed:
                self._emit_action(state, "subscribe", ",".join(newly_subscribed))

            return newly_subscribed

    def unsubscribe(
        self,
        ws_url: str,
        channel: str,
        topics: Iterable[str],
        publisher: MicroBatchPublisher | None = None,
    ) -> list[str]:
        """Decrement reference counts for `topics` and send an unsubscribe frame for topics reaching zero."""
        topic_list = [t.strip() for t in topics if t and t.strip()]
        if not topic_list:
            return []

        with self._lock:
            state = self._endpoints.get(ws_url)
            if state is None:
                return []

            if publisher is not None and channel in state.channel_publishers:
                pubs = state.channel_publishers[channel]
                if publisher in pubs:
                    pubs.remove(publisher)

            fully_unsubscribed: list[str] = []
            for topic in topic_list:
                prev = state.topic_ref_counts.get(topic, 0)
                if prev <= 1:
                    if topic in state.topic_ref_counts:
                        del state.topic_ref_counts[topic]
                    if prev == 1:
                        fully_unsubscribed.append(topic)
                else:
                    state.topic_ref_counts[topic] = prev - 1

            if fully_unsubscribed:
                self._emit_action(state, "unsubscribe", ",".join(fully_unsubscribed))

            return fully_unsubscribed

    def active_topics(self, ws_url: str) -> frozenset[str]:
        """Return the set of currently active (ref_count > 0) topics on `ws_url`."""
        with self._lock:
            state = self._endpoints.get(ws_url)
            if state is None:
                return frozenset()
            return frozenset(t for t, c in state.topic_ref_counts.items() if c > 0)

    def outbox_messages(self, ws_url: str) -> list[dict[str, Any]]:
        """Return a copy of sent control messages for `ws_url`."""
        with self._lock:
            state = self._endpoints.get(ws_url)
            return list(state.outbox) if state is not None else []

    def simulate_reconnect(self, ws_url: str) -> list[dict[str, Any]]:
        """Emit auth and resubscribe control frames for all active topics on `ws_url` after reconnect."""
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
        """Decode a raw WebSocket JSON frame and dispatch grouped event rows to registered publishers."""
        if isinstance(raw_payload, (str, bytes)):
            parsed = json.loads(raw_payload)
        else:
            parsed = raw_payload

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
                if status_val in ("auth_failed", "error", "not_authorized") and (
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
                state = self._endpoints.get(ws_url)
                pubs = (
                    list(state.channel_publishers.get(ev_type, ()))
                    if state is not None
                    else []
                )
            for pub in pubs:
                total_enqueued += pub.enqueue(rows)

        return total_enqueued

    def close(self) -> None:
        """Shut down all WebSocket subscriptions and stop the background event loop thread."""
        with self._lock:
            self._closed = True
            self._endpoints.clear()
            loop = self._loop
            thread = self._loop_thread
            self._loop = None
            self._loop_thread = None

        if loop is not None and not loop.is_closed():
            loop.call_soon_threadsafe(loop.stop)
        if thread is not None and thread.is_alive():
            thread.join(timeout=2.0)
