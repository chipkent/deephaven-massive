"""Unit and integration tests for WebSocketMultiplexer and ReactiveWatchlistController."""

from __future__ import annotations

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


def test_ws_multiplexer_ref_counting_and_dispatch(dh_exec_ctx: Any) -> None:
    sent_frames: list[tuple[str, dict[str, Any]]] = []
    cfg = MassiveConfig(api_key="ws-secret")
    mux = WebSocketMultiplexer(
        cfg, send_hook=lambda url, payload: sent_frames.append((url, payload))
    )
    ws_url = "wss://socket.massive.com/stocks"
    norm = SymbolNormalizer("stocks")

    pub1 = MicroBatchPublisher("mux_pub1", TRADES_SCHEMA, norm, dh_exec_ctx)
    pub2 = MicroBatchPublisher("mux_pub2", TRADES_SCHEMA, norm, dh_exec_ctx)

    # Unknown ws_url queries
    assert mux.active_topics(ws_url) == frozenset()
    assert mux.outbox_messages(ws_url) == []
    assert mux.unsubscribe(ws_url, "T", ["T.AAPL"]) == []
    assert mux. subscribe(ws_url, "T", ["", "   "]) == []
    assert mux.unsubscribe(ws_url, "T", ["", "   "]) == []

    # Register & unregister publisher directly
    mux.register_publisher(ws_url, "T", pub1)
    mux.register_publisher(ws_url, "T", pub1)  # duplicate is no-op
    mux.unregister_publisher(ws_url, "T", pub1)
    mux.unregister_publisher("wss://unknown", "T", pub1)

    # First subscription: T.AAPL, T.MSFT
    newly1 = mux.subscribe(ws_url, "T", ["T.AAPL", "T.MSFT"], pub1)
    assert newly1 == ["T.AAPL", "T.MSFT"]
    assert mux.active_topics(ws_url) == frozenset({"T.AAPL", "T.MSFT"})

    # Overlapping subscription from pub2: T.AAPL (ref_count -> 2), T.NVDA (ref_count -> 1)
    newly2 = mux.subscribe(ws_url, "T", ["T.AAPL", "T.NVDA"], pub2)
    assert newly2 == ["T.NVDA"]
    assert mux.active_topics(ws_url) == frozenset({"T.AAPL", "T.MSFT", "T.NVDA"})

    # Unsubscribe T.AAPL from pub1 -> ref_count drops to 1, so NO unsubscribe frame is sent
    unsub1 = mux.unsubscribe(ws_url, "T", ["T.AAPL"], pub1)
    assert unsub1 == []
    # Second unsubscribe with pub1 when pub1 is already removed from channel_publishers
    assert mux.unsubscribe(ws_url, "T", ["T.IBM"], pub1) == []
    assert "T.AAPL" in mux.active_topics(ws_url)

    # Unsubscribe T.AAPL and non-existent T.IBM from pub2 -> T.AAPL drops to 0 and emits unsubscribe
    unsub2 = mux.unsubscribe(ws_url, "T", ["T.AAPL", "T.IBM"], pub2)
    assert unsub2 == ["T.AAPL"]
    assert "T.AAPL" not in mux.active_topics(ws_url)

    # Reconnect simulation emits auth + subscribe for remaining active topics
    reconnect_frames = mux.simulate_reconnect(ws_url)
    assert reconnect_frames[0] == {"action": "auth", "params": "ws-secret"}
    assert reconnect_frames[1] == {"action": "subscribe", "params": "T.MSFT,T.NVDA"}

    # Reconnect on endpoint with no active topics only emits auth
    empty_reconnect = mux.simulate_reconnect("wss://socket.massive.com/forex")
    assert empty_reconnect == [{"action": "auth", "params": "ws-secret"}]
    assert len(mux.outbox_messages(ws_url)) >= 4

    # Re-register pub1 and test raw JSON message dispatch
    mux.register_publisher(ws_url, "T", pub1)
    enq_json = mux.handle_raw_message(
        ws_url,
        json.dumps(
            [
                {"ev": "status", "status": "connected", "message": "Connected"},
                {"ev": "status", "status": "auth_success", "message": "Authenticated"},
                {"no_ev": True},
                {"ev": "T", "sym": "MSFT", "p": 310.0, "s": 50, "t": 1700000000000000000},
            ]
        ).encode("utf-8"),
    )
    assert enq_json == 1
    assert pub1.flush_now() == 1

    # Single dict message and non-mapping message
    assert (
        mux.handle_raw_message(
            ws_url, {"ev": "T", "sym": "NVDA", "p": 910.0, "s": 10}
        )
        == 1
    )
    assert mux.handle_raw_message("wss://unregistered", {"ev": "T", "sym": "AAPL", "p": 1.0}) == 0
    assert mux.handle_raw_message(ws_url, 12345) == 0  # type: ignore[arg-type]

    # Entitlement error on status event
    with pytest.raises(MassiveEntitlementError, match="WebSocket entitlement error"):
        mux.handle_raw_message(
            ws_url,
            [{"ev": "status", "status": "auth_failed", "message": "Not authorized for channel"}],
        )

    # Test background event loop scheduling via call_soon_threadsafe
    done_ev = threading.Event()
    mux.call_soon_threadsafe(done_ev.set)
    assert done_ev.wait(timeout=2.0)
    # Second call reuses existing loop
    done_ev2 = threading.Event()
    mux.call_soon_threadsafe(done_ev2.set)
    assert done_ev2.wait(timeout=2.0)

    mux.close()


def test_ws_multiplexer_options_1000_contract_cap() -> None:
    cfg = MassiveConfig(api_key="ws-secret")
    mux = WebSocketMultiplexer(cfg, send_hook=lambda _u, _p: None)
    opt_url = "wss://socket.massive.com/options"

    # Wildcard does not count toward 1000-contract limit
    mux.subscribe(opt_url, "T", ["T.O:*"])

    # Subscribe 1,000 contracts -> succeeds
    topics_1000 = [f"T.O:AAPL250117C{i:08d}" for i in range(1, MAX_OPTION_CONTRACTS + 1)]
    mux.subscribe(opt_url, "T", topics_1000)

    # Subscribing the 1,001st contract raises ValueError
    with pytest.raises(ValueError, match="1000-contract subscription limit"):
        mux.subscribe(opt_url, "T", ["T.O:AAPL250117C00999999"])

    mux.close()


def test_ws_multiplexer_live_connector_and_reconnect_lifecycle(
    dh_exec_ctx: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    import asyncio
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

    class _FakeWS:
        def __init__(self, mode: str) -> None:
            self.mode = mode
            self._step = 0

        async def __aenter__(self) -> _FakeWS:
            if self.mode == "transient_fail":
                raise OSError("transient connection drop")
            return self

        async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
            return False

        async def send(self, msg: str) -> None:
            payload = json.loads(msg)
            ws_sent.append(payload)
            if payload.get("action") == "subscribe" and "T.MSFT" in payload.get("params", ""):
                post_sub_event.set()

        def __aiter__(self) -> _FakeWS:
            return self

        async def __anext__(self) -> str:
            self._step += 1
            if self.mode == "entitlement_fail":
                if self._step == 1:
                    return json.dumps(
                        [{"ev": "status", "status": "auth_failed", "message": "Not authorized"}]
                    )
                raise StopAsyncIteration
            if self._step == 1:
                return json.dumps([{"ev": "status", "status": "auth_success"}])
            if self._step == 2:
                auth_ok_event.set()
                return json.dumps(
                    [{"ev": "T", "sym": "AAPL", "p": 155.0, "s": 10, "t": 1700000000000000000}]
                )
            if self._step == 3:
                for _ in range(100):
                    if unsub_done_event.is_set():
                        break
                    await asyncio.sleep(0.01)
                raise StopAsyncIteration
            raise StopAsyncIteration

    def _connector(_url: str) -> _FakeWS:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return _FakeWS("transient_fail")
        return _FakeWS("normal")

    cfg = MassiveConfig(api_key="live-key")
    mux = WebSocketMultiplexer(
        cfg, ws_connector=_connector, reconnect_delay_s=0.01
    )

    # First subscribe starts _run_connection on background loop
    mux.subscribe(ws_url, "T", ["T.AAPL"], pub)
    # Calling _ensure_connection while already connecting is a no-op
    mux._ensure_connection(ws_url)

    assert auth_ok_event.wait(timeout=3.0)
    # Now authenticated: subscribing a second topic sends post-auth frame directly over active_ws
    mux.subscribe(ws_url, "T", ["T.MSFT"], pub)
    assert post_sub_event.wait(timeout=3.0)

    # Unsubscribe all topics so next loop iteration sees has_topics == False and exits
    mux.unsubscribe(ws_url, "T", ["T.AAPL", "T.MSFT"], pub)
    unsub_done_event.set()

    for _ in range(100):
        with mux._lock:
            st = mux._endpoints.get(ws_url)
            if st is not None and not st.connecting:
                break
        time.sleep(0.01)

    assert any(m.get("action") == "auth" for m in ws_sent)
    assert any(m.get("action") == "subscribe" and m.get("params") == "T.AAPL" for m in ws_sent)
    assert any(m.get("action") == "subscribe" and m.get("params") == "T.MSFT" for m in ws_sent)
    assert any(m.get("action") == "unsubscribe" for m in ws_sent)
    assert pub.flush_now() == 1
    mux.close()

    # Also test entitlement failure aborting _run_connection without infinite retry
    mux_ent = WebSocketMultiplexer(
        cfg,
        ws_connector=lambda _u: _FakeWS("entitlement_fail"),
        reconnect_delay_s=0.01,
    )
    mux_ent.subscribe(ws_url, "T", ["T.AAPL"], pub)
    for _ in range(100):
        with mux_ent._lock:
            st_ent = mux_ent._endpoints.get(ws_url)
            if st_ent is not None and isinstance(st_ent.last_error, MassiveEntitlementError) and not st_ent.connecting:
                break
        time.sleep(0.01)
    assert isinstance(mux_ent._endpoints[ws_url].last_error, MassiveEntitlementError)
    mux_ent.close()

    # Also test _run_connection exiting when self._closed becomes True during reconnect backoff
    closed_mux = WebSocketMultiplexer(
        cfg,
        ws_connector=lambda _u: _FakeWS("transient_fail"),
        reconnect_delay_s=0.01,
    )
    closed_mux._closed = True
    asyncio.run(closed_mux._run_connection(ws_url))


def test_reactive_watchlist_controller_lifecycle(dh_exec_ctx: Any) -> None:
    from deephaven import dtypes as dht
    from deephaven import input_table

    cfg = MassiveConfig(api_key="ws-secret")
    mux = WebSocketMultiplexer(cfg, send_hook=lambda _u, _p: None)
    ws_url = "wss://socket.massive.com/stocks"
    norm = SymbolNormalizer("stocks")
    pub = MicroBatchPublisher("wl_pub", TRADES_SCHEMA, norm, dh_exec_ctx)

    with exec_ctx_scope(dh_exec_ctx):
        wl_table = input_table(col_defs={"Symbol": dht.string}, key_cols=["Symbol"])
        # Pre-populate AAPL before creating controller so do_replay=True picks it up
        init_pa = pa.Table.from_pydict({"Symbol": ["AAPL"]})
        wl_table.add(arrow_to_deephaven_table(init_pa, dh_exec_ctx))

    seeded_batches: list[list[str]] = []

    def _seed_fn(syms: list[str]) -> list[dict[str, Any]]:
        seeded_batches.append(list(syms))
        return [{"sym": s, "p": 100.0, "s": 10, "i": f"seed-{s}", "t": 1700000000000000000} for s in syms]

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

    assert "AAPL" in ctrl.active_symbols
    assert "T.AAPL" in mux.active_topics(ws_url)
    assert "AAPL" in (pub.allowed_symbols or set())
    assert ["AAPL"] in seeded_batches

    # Add MSFT and NVDA to watchlist
    with exec_ctx_scope(dh_exec_ctx):
        add_pa = pa.Table.from_pydict({"Symbol": ["MSFT", "NVDA"]})
        wl_table.add(arrow_to_deephaven_table(add_pa, dh_exec_ctx))

    for _ in range(100):
        ctrl.wait_idle(timeout=1.0)
        if ctrl.active_symbols == frozenset({"AAPL", "MSFT", "NVDA"}):
            break
        time.sleep(0.02)
    assert ctrl.active_symbols == frozenset({"AAPL", "MSFT", "NVDA"})
    assert mux.active_topics(ws_url) == frozenset({"T.AAPL", "T.MSFT", "T.NVDA"})

    # Delete AAPL from watchlist -> unsubscribed and removed from active_subs_table
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

    # Test helper edge cases on ReactiveWatchlistController
    assert ctrl._extract_symbols(None) == []
    assert ctrl._extract_symbols({"OtherCol": ["AAPL"]}) == []
    assert ctrl._extract_symbols({"Symbol": [None, "  ", "goog"]}) == ["GOOG"]

    # Fake update with no net symbol changes (modified symbol unchanged)
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

    # Test wait_idle timeout when _pending_ops > 0
    with ctrl._idle_Condition:
        ctrl._pending_ops = 1
    assert ctrl.wait_idle(timeout=0.001) is False
    with ctrl._idle_Condition:
        ctrl._pending_ops = 0

    # Stop controller -> unsubscribes all remaining active symbols
    ctrl.stop()
    ctrl.stop()  # idempotent second stop
    ctrl.apply_diff(["GOOG"], [])  # ignored after stop()
    assert ctrl.active_symbols == frozenset()
    assert mux.active_topics(ws_url) == frozenset()

    # Also test ReactiveWatchlistController with exec_ctx=None and seed_fn returning empty list
    with exec_ctx_scope(dh_exec_ctx):
        wl2 = input_table(col_defs={"Symbol": dht.string}, key_cols=["Symbol"])
        ctrl2 = ReactiveWatchlistController(
            watchlist_table=wl2,
            ws_url=ws_url,
            channel="T",
            publisher=pub,
            ws_multiplexer=mux,
            symbol_normalizer=norm,
            exec_ctx=None,
            seed_fn=lambda _: [],
        )
        ctrl2.apply_diff(["IBM"], [])
        ctrl2.apply_diff([], ["IBM"])
        ctrl2.stop()

    mux.close()
