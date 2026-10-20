"""Unit and integration tests for WebSocketMultiplexer and ReactiveWatchlistController."""

from __future__ import annotations

import asyncio
import json
import threading
import time
from typing import Any

import pyarrow as pa
import pytest

from deephaven_massive.config import MassiveConfig, MassiveEntitlementError
from deephaven_massive.engine.arrow_bridge import (
    arrow_to_deephaven_table,
    exec_ctx_scope,
)
from deephaven_massive.engine.publisher import MicroBatchPublisher
from deephaven_massive.engine.symbols import SymbolNormalizer
from deephaven_massive.engine.watchlist import ReactiveWatchlistController
from deephaven_massive.engine.ws_multiplexer import (
    MAX_OPTION_CONTRACTS,
    WebSocketMultiplexer,
    _default_ws_connector,
)
from deephaven_massive.schemas.market import TRADES_SCHEMA
from tests.conftest import dh_to_pydict


def test_ws_multiplexer_topic_ref_counting_and_publisher_registration(
    dh_exec_ctx: Any,
) -> None:
    """Verify WebSocketMultiplexer reference-counts shared topics and cleans up unregistered publishers."""
    cfg = MassiveConfig(api_key="ws-secret")
    mux = WebSocketMultiplexer(cfg, send_hook=lambda _u, _p: None)
    ws_url = "wss://socket.massive.com/stocks"
    norm = SymbolNormalizer("stocks")

    pub1 = MicroBatchPublisher("mux_pub1", TRADES_SCHEMA, norm, dh_exec_ctx)
    pub2 = MicroBatchPublisher("mux_pub2", TRADES_SCHEMA, norm, dh_exec_ctx)

    assert mux.active_topics(ws_url) == frozenset()
    assert mux.outbox_messages(ws_url) == []
    assert mux.unsubscribe(ws_url, "T", ["T.AAPL"]) == []
    assert mux.subscribe(ws_url, "T", ["", "   "]) == []
    assert mux.unsubscribe(ws_url, "T", ["", "   "]) == []

    mux.register_publisher(ws_url, "T", pub1)
    mux.register_publisher(ws_url, "T", pub1)  # duplicate is no-op
    mux.unregister_publisher(ws_url, "T", pub1)
    mux.unregister_publisher("wss://unknown", "T", pub1)

    newly1 = mux.subscribe(ws_url, "T", ["T.AAPL", "T.MSFT"], pub1)
    assert newly1 == ["T.AAPL", "T.MSFT"]
    assert mux.subscribe(ws_url, "T", ["T.AAPL"], pub1) == []
    assert mux.active_topics(ws_url) == frozenset({"T.AAPL", "T.MSFT"})

    newly2 = mux.subscribe(ws_url, "T", ["T.AAPL", "T.NVDA"], pub2)
    assert newly2 == ["T.NVDA"]
    assert mux.active_topics(ws_url) == frozenset({"T.AAPL", "T.MSFT", "T.NVDA"})

    assert mux.unsubscribe(ws_url, "T", ["T.AAPL"], pub1) == []
    assert mux.unsubscribe(ws_url, "T", ["T.AAPL", "T.IBM"], pub1) == []
    assert mux.unsubscribe(ws_url, "T", ["T.IBM"], None) == []
    assert "T.AAPL" in mux.active_topics(ws_url)

    assert mux.unsubscribe(ws_url, "T", ["T.AAPL", "T.IBM"], pub2) == ["T.AAPL"]
    assert "T.AAPL" not in mux.active_topics(ws_url)

    mux.unregister_publisher(ws_url, "T", pub2)
    assert "T.NVDA" not in mux.active_topics(ws_url)
    assert mux.unsubscribe(ws_url, "T", ["T.MSFT"], pub2) == []
    mux.close()


def test_ws_multiplexer_simulate_reconnect_and_outbox(dh_exec_ctx: Any) -> None:
    """Verify simulate_reconnect re-authenticates and re-subscribes all active topics."""
    cfg = MassiveConfig(api_key="ws-secret")
    mux = WebSocketMultiplexer(cfg, send_hook=lambda _u, _p: None)
    ws_url = "wss://socket.massive.com/stocks"
    norm = SymbolNormalizer("stocks")
    pub = MicroBatchPublisher("mux_rec_pub", TRADES_SCHEMA, norm, dh_exec_ctx)

    mux.subscribe(ws_url, "T", ["T.MSFT"], pub)
    reconnect_frames = mux.simulate_reconnect(ws_url)
    assert reconnect_frames == [
        {"action": "auth", "params": "ws-secret"},
        {"action": "subscribe", "params": "T.MSFT"},
    ]
    assert len(mux.outbox_messages(ws_url)) == 3

    empty_reconnect = mux.simulate_reconnect("wss://socket.massive.com/forex")
    assert empty_reconnect == [{"action": "auth", "params": "ws-secret"}]
    mux.close()


def test_ws_multiplexer_raw_message_dispatch_and_entitlement_error(
    dh_exec_ctx: Any,
) -> None:
    """Verify handle_raw_message dispatches events by channel and raises MassiveEntitlementError on auth failure."""
    cfg = MassiveConfig(api_key="ws-secret")
    mux = WebSocketMultiplexer(cfg, send_hook=lambda _u, _p: None)
    ws_url = "wss://socket.massive.com/stocks"
    norm = SymbolNormalizer("stocks")
    pub = MicroBatchPublisher("mux_msg_pub", TRADES_SCHEMA, norm, dh_exec_ctx)
    mux.subscribe(ws_url, "T", ["T.MSFT"], pub)

    enq_json = mux.handle_raw_message(
        ws_url,
        json.dumps(
            [
                {"ev": "status", "status": "connected", "message": "Connected"},
                {"ev": "status", "status": "auth_success", "message": "Authenticated"},
                {"no_ev": True},
                {
                    "ev": "T",
                    "sym": "MSFT",
                    "p": 310.0,
                    "s": 50,
                    "t": 1700000000000000000,
                },
            ]
        ).encode("utf-8"),
    )
    assert enq_json == 1
    assert pub.flush_now() == 1

    assert (
        mux.handle_raw_message(
            ws_url, {"ev": "T", "sym": "NVDA", "p": 910.0, "s": 10}
        )
        == 1
    )
    assert (
        mux.handle_raw_message(
            "wss://unregistered", {"ev": "T", "sym": "AAPL", "p": 1.0}
        )
        == 0
    )
    assert mux.handle_raw_message(ws_url, 12345) == 0  # type: ignore[arg-type]

    with pytest.raises(MassiveEntitlementError, match="WebSocket entitlement error"):
        mux.handle_raw_message(
            ws_url,
            [
                {
                    "ev": "status",
                    "status": "auth_failed",
                    "message": "Not authorized for channel",
                }
            ],
        )
    mux.close()


def test_ws_multiplexer_background_event_loop_scheduling() -> None:
    """Verify call_soon_threadsafe boots and reuses the background asyncio event loop thread."""
    cfg = MassiveConfig(api_key="ws-secret")
    mux = WebSocketMultiplexer(cfg, send_hook=lambda _u, _p: None)

    done_ev1 = threading.Event()
    mux.call_soon_threadsafe(done_ev1.set)
    assert done_ev1.wait(timeout=2.0)

    done_ev2 = threading.Event()
    mux.call_soon_threadsafe(done_ev2.set)
    assert done_ev2.wait(timeout=2.0)
    mux.close()


def test_ws_multiplexer_options_1000_contract_limit() -> None:
    """Verify options endpoints enforce the 1,000-contract limit while exempting wildcard topics."""
    cfg = MassiveConfig(api_key="ws-secret")
    mux = WebSocketMultiplexer(cfg, send_hook=lambda _u, _p: None)
    opt_url = "wss://socket.massive.com/options"

    mux.subscribe(opt_url, "T", ["T.O:*"])
    topics_1000 = [
        f"T.O:AAPL250117C{i:08d}" for i in range(1, MAX_OPTION_CONTRACTS + 1)
    ]
    mux.subscribe(opt_url, "T", topics_1000)

    with pytest.raises(ValueError, match="1000-contract subscription limit"):
        mux.subscribe(opt_url, "T", ["T.O:AAPL250117C00999999"])

    mux.close()


class _FakeWS:
    """Injectable async WebSocket context manager for testing live connection loops."""

    def __init__(
        self,
        mode: str,
        ws_sent: list[dict[str, Any]] | None = None,
        auth_ok_event: threading.Event | None = None,
        post_sub_event: threading.Event | None = None,
        unsub_done_event: threading.Event | None = None,
    ) -> None:
        self.mode = mode
        self.ws_sent = ws_sent if ws_sent is not None else []
        self.auth_ok_event = auth_ok_event
        self.post_sub_event = post_sub_event
        self.unsub_done_event = unsub_done_event
        self._step = 0

    async def __aenter__(self) -> _FakeWS:
        if self.mode == "transient_fail":
            raise OSError("transient connection drop")
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        return False

    async def send(self, msg: str) -> None:
        payload = json.loads(msg)
        self.ws_sent.append(payload)
        if (
            self.post_sub_event is not None
            and payload.get("action") == "subscribe"
            and "T.MSFT" in payload.get("params", "")
        ):
            self.post_sub_event.set()

    def __aiter__(self) -> _FakeWS:
        return self

    async def __anext__(self) -> str:
        self._step += 1
        if self.mode == "entitlement_fail":
            if self._step == 1:
                return json.dumps(
                    [
                        {
                            "ev": "status",
                            "status": "auth_failed",
                            "message": "Not authorized",
                        }
                    ]
                )
            raise StopAsyncIteration
        if self._step == 1:
            return json.dumps([{"ev": "status", "status": "auth_success"}])
        if self._step == 2:
            if self.auth_ok_event is not None:
                self.auth_ok_event.set()
            return json.dumps(
                [
                    {
                        "ev": "T",
                        "sym": "AAPL",
                        "p": 155.0,
                        "s": 10,
                        "t": 1700000000000000000,
                    }
                ]
            )
        if self._step == 3:
            for _ in range(100):
                if self.unsub_done_event is not None and self.unsub_done_event.is_set():
                    break
                await asyncio.sleep(0.01)
            raise StopAsyncIteration
        raise StopAsyncIteration


def test_ws_multiplexer_live_connector_and_reconnect_recovery(
    dh_exec_ctx: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify _run_connection recovers from transient disconnects, authenticates, and sends post-auth frames."""
    import websockets

    monkeypatch.setattr(websockets, "connect", lambda url: f"connected:{url}")
    assert _default_ws_connector("wss://example.com") == "connected:wss://example.com"

    ws_url = "wss://socket.massive.com/stocks"
    norm = SymbolNormalizer("stocks")
    pub = MicroBatchPublisher("live_ws_pub", TRADES_SCHEMA, norm, dh_exec_ctx)

    ws_sent: list[dict[str, Any]] = []
    auth_ok_event = threading.Event()
    post_sub_event = threading.Event()
    unsub_done_event = threading.Event()
    attempts = 0

    def _connector(_url: str) -> _FakeWS:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return _FakeWS("transient_fail", ws_sent)
        return _FakeWS(
            "normal", ws_sent, auth_ok_event, post_sub_event, unsub_done_event
        )

    cfg = MassiveConfig(api_key="live-key")
    mux = WebSocketMultiplexer(cfg, ws_connector=_connector, reconnect_delay_s=0.01)

    mux.subscribe(ws_url, "T", ["T.AAPL"], pub)
    mux._ensure_connection(ws_url)  # no-op while already connecting

    assert auth_ok_event.wait(timeout=3.0)
    mux.subscribe(ws_url, "T", ["T.MSFT"], pub)
    assert post_sub_event.wait(timeout=3.0)

    mux.unsubscribe(ws_url, "T", ["T.AAPL", "T.MSFT"], pub)
    unsub_done_event.set()

    for _ in range(100):
        with mux._lock:
            st = mux._endpoints.get(ws_url)
            if st is not None and not st.connecting:
                break
        time.sleep(0.01)

    assert any(m.get("action") == "auth" for m in ws_sent)
    assert any(
        m.get("action") == "subscribe" and m.get("params") == "T.AAPL" for m in ws_sent
    )
    assert any(
        m.get("action") == "subscribe" and m.get("params") == "T.MSFT" for m in ws_sent
    )
    assert any(m.get("action") == "unsubscribe" for m in ws_sent)
    assert pub.flush_now() == 1
    mux.close()


def test_ws_multiplexer_live_entitlement_failure_aborts_reconnect(
    dh_exec_ctx: Any,
) -> None:
    """Verify _run_connection halts reconnection when an entitlement error occurs or when closed."""
    ws_url = "wss://socket.massive.com/stocks"
    norm = SymbolNormalizer("stocks")
    pub = MicroBatchPublisher("ent_ws_pub", TRADES_SCHEMA, norm, dh_exec_ctx)
    cfg = MassiveConfig(api_key="live-key")

    mux_ent = WebSocketMultiplexer(
        cfg,
        ws_connector=lambda _u: _FakeWS("entitlement_fail"),
        reconnect_delay_s=0.01,
    )
    mux_ent.subscribe(ws_url, "T", ["T.AAPL"], pub)
    for _ in range(100):
        with mux_ent._lock:
            st_ent = mux_ent._endpoints.get(ws_url)
            if (
                st_ent is not None
                and isinstance(st_ent.last_error, MassiveEntitlementError)
                and not st_ent.connecting
            ):
                break
        time.sleep(0.01)
    assert isinstance(mux_ent._endpoints[ws_url].last_error, MassiveEntitlementError)
    mux_ent.close()

    closed_mux = WebSocketMultiplexer(
        cfg,
        ws_connector=lambda _u: _FakeWS("transient_fail"),
        reconnect_delay_s=0.01,
    )
    closed_mux._closed = True
    asyncio.run(closed_mux._run_connection(ws_url))


def test_reactive_watchlist_initial_replay_and_seeding(dh_exec_ctx: Any) -> None:
    """Verify ReactiveWatchlistController replays initial watchlist rows and executes seed_fn on startup."""
    from deephaven import dtypes as dht
    from deephaven import input_table

    cfg = MassiveConfig(api_key="ws-secret")
    mux = WebSocketMultiplexer(cfg, send_hook=lambda _u, _p: None)
    ws_url = "wss://socket.massive.com/stocks"
    norm = SymbolNormalizer("stocks")
    pub = MicroBatchPublisher("wl_replay_pub", TRADES_SCHEMA, norm, dh_exec_ctx)

    with exec_ctx_scope(dh_exec_ctx):
        wl_table = input_table(col_defs={"Symbol": dht.string}, key_cols=["Symbol"])
        init_pa = pa.Table.from_pydict({"Symbol": ["AAPL"]})
        wl_table.add(arrow_to_deephaven_table(init_pa, dh_exec_ctx))

    seeded_batches: list[list[str]] = []

    def _seed_fn(syms: list[str]) -> list[dict[str, Any]]:
        seeded_batches.append(list(syms))
        return [
            {
                "sym": s,
                "p": 100.0,
                "s": 10,
                "i": f"seed-{s}",
                "t": 1700000000000000000,
            }
            for s in syms
        ]

    ctrl = ReactiveWatchlistController(
        watchlist_table=wl_table,
        ws_url=ws_url,
        channel="T",
        publisher=pub,
        ws_multiplexer=mux,
        symbol_normalizer=norm,
        exec_ctx=dh_exec_ctx,
        seed_fn=_seed_fn,
    )
    assert ctrl.wait_idle(timeout=5.0)
    ctrl.start()  # idempotent second start()

    assert ctrl.active_symbols == frozenset({"AAPL"})
    assert mux.active_topics(ws_url) == frozenset({"T.AAPL"})
    assert "AAPL" in (pub.allowed_symbols or set())
    assert ["AAPL"] in seeded_batches

    ctrl.stop()
    mux.close()


def test_reactive_watchlist_dynamic_add_and_delete_eviction(dh_exec_ctx: Any) -> None:
    """Verify adding and deleting symbols in a Deephaven watchlist table dynamically updates topics and filters."""
    from deephaven import dtypes as dht
    from deephaven import input_table

    cfg = MassiveConfig(api_key="ws-secret")
    mux = WebSocketMultiplexer(cfg, send_hook=lambda _u, _p: None)
    ws_url = "wss://socket.massive.com/stocks"
    norm = SymbolNormalizer("stocks")
    pub = MicroBatchPublisher("wl_diff_pub", TRADES_SCHEMA, norm, dh_exec_ctx)

    with exec_ctx_scope(dh_exec_ctx):
        wl_table = input_table(col_defs={"Symbol": dht.string}, key_cols=["Symbol"])

    ctrl = ReactiveWatchlistController(
        watchlist_table=wl_table,
        ws_url=ws_url,
        channel="T",
        publisher=pub,
        ws_multiplexer=mux,
        symbol_normalizer=norm,
        exec_ctx=dh_exec_ctx,
    )
    assert ctrl.wait_idle(timeout=5.0)

    with exec_ctx_scope(dh_exec_ctx):
        add_pa = pa.Table.from_pydict({"Symbol": ["AAPL", "MSFT", "NVDA"]})
        wl_table.add(arrow_to_deephaven_table(add_pa, dh_exec_ctx))

    for _ in range(100):
        ctrl.wait_idle(timeout=1.0)
        if ctrl.active_symbols == frozenset({"AAPL", "MSFT", "NVDA"}):
            break
        time.sleep(0.02)
    assert ctrl.active_symbols == frozenset({"AAPL", "MSFT", "NVDA"})
    assert mux.active_topics(ws_url) == frozenset({"T.AAPL", "T.MSFT", "T.NVDA"})

    with exec_ctx_scope(dh_exec_ctx):
        del_pa = pa.Table.from_pydict({"Symbol": ["AAPL"]})
        wl_table.delete(arrow_to_deephaven_table(del_pa, dh_exec_ctx))

    for _ in range(100):
        ctrl.wait_idle(timeout=1.0)
        if ctrl.active_symbols == frozenset({"MSFT", "NVDA"}):
            break
        time.sleep(0.02)
    assert ctrl.active_symbols == frozenset({"MSFT", "NVDA"})
    assert "T.AAPL" not in mux.active_topics(ws_url)
    assert "AAPL" not in (pub.allowed_symbols or set())
    subs_dict = dh_to_pydict(ctrl.active_subs_table)
    assert set(subs_dict["Symbol"]) == {"MSFT", "NVDA"}

    ctrl.stop()
    mux.close()


def test_reactive_watchlist_edge_cases_and_shutdown(dh_exec_ctx: Any) -> None:
    """Verify ReactiveWatchlistController handles empty/unchanged updates, wait_idle timeouts, and stop()."""
    from deephaven import dtypes as dht
    from deephaven import input_table

    cfg = MassiveConfig(api_key="ws-secret")
    mux = WebSocketMultiplexer(cfg, send_hook=lambda _u, _p: None)
    ws_url = "wss://socket.massive.com/stocks"
    norm = SymbolNormalizer("stocks")
    pub = MicroBatchPublisher("wl_edge_pub", TRADES_SCHEMA, norm, dh_exec_ctx)

    with exec_ctx_scope(dh_exec_ctx):
        wl_table = input_table(col_defs={"Symbol": dht.string}, key_cols=["Symbol"])
        ctrl = ReactiveWatchlistController(
            watchlist_table=wl_table,
            ws_url=ws_url,
            channel="T",
            publisher=pub,
            ws_multiplexer=mux,
            symbol_normalizer=norm,
            exec_ctx=None,
            seed_fn=lambda _: [],
        )

    assert ctrl._extract_symbols(None) == []
    assert ctrl._extract_symbols({"OtherCol": ["AAPL"]}) == []
    assert ctrl._extract_symbols({"Symbol": [None, "  ", "goog"]}) == ["GOOG"]

    class _FakeUpdate:
        def added(self) -> dict[str, list[str]]:
            return {}

        def removed(self) -> dict[str, list[str]]:
            return {}

        def modified(self) -> dict[str, list[str]]:
            return {"Symbol": ["MSFT"]}

        def modified_prev(self) -> dict[str, list[str]]:
            return {"Symbol": ["MSFT"]}

    ctrl._on_table_update(_FakeUpdate(), False)

    with ctrl._idle_Condition:
        ctrl._pending_ops = 1
    assert ctrl.wait_idle(timeout=0.001) is False
    with ctrl._idle_Condition:
        ctrl._pending_ops = 0

    ctrl.apply_diff(["IBM"], [])
    assert ctrl.active_symbols == frozenset({"IBM"})
    ctrl.apply_diff([], ["IBM"])
    assert ctrl.active_symbols == frozenset()

    ctrl.stop()
    ctrl.stop()  # idempotent second stop
    ctrl.apply_diff(["GOOG"], [])  # ignored after stop()
    assert ctrl.active_symbols == frozenset()
    mux.close()
