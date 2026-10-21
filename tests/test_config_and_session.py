"""Unit tests for MassiveConfig, MassiveEntitlementError, and MassiveSession."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from unittest.mock import MagicMock

import pytest

from deephaven_massive.config import MassiveConfig, MassiveEntitlementError
from deephaven_massive.session import MassiveSession, _ensure_deephaven_server
from tests.conftest import dh_to_pylist


def test_config_from_args_env_resolution_and_fallbacks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify MassiveConfig resolves API and S3 credentials from arguments and environment variables."""
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
    monkeypatch.delenv("POLYGON_API_KEY", raising=False)
    monkeypatch.delenv("MASSIVE_S3_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("MASSIVE_S3_SECRET_ACCESS_KEY", raising=False)

    monkeypatch.setenv("POLYGON_API_KEY", "poly-fallback-key")
    cfg_poly = MassiveConfig.from_args()
    assert cfg_poly.api_key == "poly-fallback-key"
    assert not cfg_poly.has_s3_credentials

    monkeypatch.setenv("MASSIVE_API_KEY", "massive-env-key")
    monkeypatch.setenv("MASSIVE_S3_ACCESS_KEY_ID", "s3-id")
    monkeypatch.setenv("MASSIVE_S3_SECRET_ACCESS_KEY", "s3-secret")
    cfg_env = MassiveConfig.from_args()
    assert cfg_env.api_key == "massive-env-key"
    assert cfg_env.has_s3_credentials


def test_config_from_args_validation_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify MassiveConfig rejects missing API keys and non-positive batching/retry settings."""
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
    monkeypatch.delenv("POLYGON_API_KEY", raising=False)

    with pytest.raises(ValueError, match="Massive API key is required"):
        MassiveConfig.from_args(api_key=None)

    with pytest.raises(ValueError, match="Massive API key is required"):
        MassiveConfig.from_args(api_key="   ")

    with pytest.raises(ValueError, match="flush_interval_ms must be > 0"):
        MassiveConfig.from_args(api_key="k", flush_interval_ms=0)

    with pytest.raises(ValueError, match="max_batch_size must be > 0"):
        MassiveConfig.from_args(api_key="k", max_batch_size=0)

    with pytest.raises(ValueError, match="max_retries must be >= 0"):
        MassiveConfig.from_args(api_key="k", max_retries=-1)


def test_config_resolve_ws_url_tiers_fmv_and_venues() -> None:
    """Verify WebSocket URL resolution across realtime/delayed tiers, FMV, custom hosts, and futures venues."""
    cfg = MassiveConfig.from_args(api_key="k", feed="realtime")
    assert cfg._resolve_ws_url("stocks") == "wss://socket.massive.com/stocks"
    assert (
        cfg._resolve_ws_url("options", feed_override="delayed")
        == "wss://delayed.massive.com/options"
    )
    assert cfg._resolve_ws_url("stocks", is_fmv=True) == "wss://business.massive.com/stocks"
    assert (
        cfg._resolve_ws_url("stocks", feed_override="delayed", is_fmv=True)
        == "wss://delayed-business.massive.com/stocks"
    )
    assert cfg._resolve_ws_url("futures", venue="cme") == "wss://socket.massive.com/futures/cme"
    assert (
        cfg._resolve_ws_url("stocks", feed_override="custom.host.example.com")
        == "wss://custom.host.example.com/stocks"
    )
    with pytest.raises(ValueError, match="Unsupported futures venue"):
        cfg._resolve_ws_url("futures", venue="invalid_venue")


def test_ensure_deephaven_server_auto_start_branches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify _ensure_deephaven_server respects auto_start=False and boots Server when auto_start=True."""
    from deephaven_server import Server

    orig_instance = Server.instance
    try:
        monkeypatch.setattr(Server, "instance", None)
        assert _ensure_deephaven_server(auto_start=False) is None

        started: list[int] = []

        class DummyServer:
            def __init__(self, port: int, jvm_args: list[str]) -> None:
                self.port = port
                self.jvm_args = jvm_args

            def start(self) -> None:
                started.append(self.port)
                Server.instance = orig_instance

        monkeypatch.setattr("deephaven_server.Server", DummyServer)
        monkeypatch.setattr(DummyServer, "instance", None, raising=False)
        ctx = _ensure_deephaven_server(auto_start=True, port=10099)
        assert started == [10099]
        assert ctx is not None
    finally:
        Server.instance = orig_instance


def test_session_request_json_url_building_and_param_filtering() -> None:
    """Verify _request_json constructs relative/full URLs, attaches Bearer headers, and drops None params."""
    calls: list[str] = []

    def _handler(
        url: str,
        params: Mapping[str, Any] | None,
        headers: Mapping[str, str],
    ) -> tuple[int, Any]:
        calls.append(url)
        assert headers["Authorization"] == "Bearer test-key"
        return 200, {"status": "OK", "results": [{"ok": True}]}

    with MassiveSession(
        api_key="test-key",
        http_handler=_handler,
        ws_send_hook=lambda _u, _p: None,
        sleep_fn=lambda _: None,
    ) as sess:
        res = sess._request_json("v3/trades/AAPL", {"limit": 10, "empty": None})
        assert res["results"] == [{"ok": True}]
        assert calls[-1] == "https://api.massive.com/v3/trades/AAPL?limit=10"

        res_all_none = sess._request_json("/v3/trades/AAPL", {"empty": None})
        assert res_all_none["status"] == "OK"
        assert calls[-1] == "https://api.massive.com/v3/trades/AAPL"

        res_cursor = sess._request_json(
            "https://api.massive.com/v3/trades/AAPL?cursor=abc",
            {"limit": 5},
        )
        assert res_cursor["status"] == "OK"
        assert calls[-1] == "https://api.massive.com/v3/trades/AAPL?cursor=abc&limit=5"


def test_session_request_json_retry_backoff_and_error_handling() -> None:
    """Verify _request_json retries HTTP 429/5xx responses and raises on 403/400 errors."""
    calls: list[str] = []
    sleeps: list[float] = []

    def _handler(
        url: str,
        params: Mapping[str, Any] | None,
        headers: Mapping[str, str],
    ) -> tuple[int, Any]:
        calls.append(url)
        if "entitlement_error" in url:
            return 403, {"status": "NOT_AUTHORIZED", "message": "Upgrade plan"}
        if "hard_error" in url:
            return 400, "Bad Request"
        if "flaky" in url and len(calls) < 3:
            return 429, {"error": "rate limited"}
        return 200, {"status": "OK", "results": [{"ok": True}]}

    with MassiveSession(
        api_key="test-key",
        max_retries=2,
        http_handler=_handler,
        ws_send_hook=lambda _u, _p: None,
        sleep_fn=sleeps.append,
    ) as sess:
        res_flaky = sess._request_json("/flaky")
        assert res_flaky["status"] == "OK"
        assert len(calls) == 3
        assert len(sleeps) == 2

        with pytest.raises(MassiveEntitlementError, match="Upgrade plan"):
            sess._request_json("/entitlement_error")

        with pytest.raises(RuntimeError, match="HTTP 400"):
            sess._request_json("/hard_error")


def test_session_status_and_stats_diagnostic_tables(dh_exec_ctx: Any) -> None:
    """Verify status() and stats() return Deephaven tables with accurate connection and builder diagnostics."""
    with MassiveSession(
        api_key="test-key",
        http_handler=lambda _u, _p, _h: (200, {"status": "OK", "results": []}),
        ws_send_hook=lambda _u, _p: None,
        sleep_fn=lambda _: None,
    ) as sess:
        assert sess.status().size == 0
        assert sess.stats().size == 0

        b_stream = sess.stocks.trades("AAPL")
        _ = b_stream.latest(seed=False)

        b_poll = sess.stocks.snapshots("AAPL")
        _ = b_poll.poll(interval_s=30.0)

        b_wl = sess.stocks.trades("GOOG")
        b_wl._watchlist_controller = object()  # type: ignore[assignment]
        sess._register_builder(b_wl)
        sess._register_builder(b_wl)  # idempotent registration

        status_rows = dh_to_pylist(sess.status())
        assert len(status_rows) == 1
        assert status_rows[0]["WsUrl"] == "wss://socket.massive.com/stocks"
        assert status_rows[0]["ActiveTopicCount"] == 1

        stats_rows = dh_to_pylist(sess.stats())
        modes = {row["Symbols"]: row["Mode"] for row in stats_rows}
        assert modes["AAPL"] in ("stream", "poll")
        assert modes["GOOG"] == "watchlist"
        assert {row["Mode"] for row in stats_rows} == {"stream", "poll", "watchlist"}

        b_wl._watchlist_controller = None
        sess.stop_stream(b_wl)
        sess.stop_stream(b_poll)


def test_session_stop_stream_filtering_and_lifecycle(dh_exec_ctx: Any) -> None:
    """Verify stop_stream tears down matching builders by instance, feed name, channel, or symbol."""
    with MassiveSession(
        api_key="test-key",
        http_handler=lambda _u, _p, _h: (200, {"status": "OK", "results": []}),
        ws_send_hook=lambda _u, _p: None,
        sleep_fn=lambda _: None,
    ) as sess:
        b_trades = sess.stocks.trades("AAPL")
        b_quotes = sess.stocks.quotes(["MSFT", "NVDA"])
        sess._register_builder(b_trades)
        sess._register_builder(b_quotes)

        assert sess.stop_stream("nonexistent") == 0
        assert sess.stop_stream(ws_channel="AM") == 0
        assert sess.stop_stream(symbols="GOOG") == 0
        assert sess.stop_stream(symbols=["GOOG"]) == 0

        assert sess.stop_stream(ws_channel="Q", symbols=["MSFT"]) == 1
        assert sess.stop_stream(b_trades) == 1

        sess._register_builder(b_trades)
        assert sess.stop_stream(symbols="AAPL") == 1

        sess._register_builder(b_trades)
        assert sess.stop_stream() == 1


def test_session_urllib3_pool_execution() -> None:
    """Verify the default urllib3 PoolManager code path decodes both populated and empty JSON bodies."""
    sess = MassiveSession(api_key="test-key")
    mock_resp = MagicMock()
    mock_resp.status = 200
    mock_resp.data = b'{"status": "OK", "results": []}'
    sess._http_pool = MagicMock()
    sess._http_pool.request.return_value = mock_resp

    data = sess._request_json("/v1/marketstatus/now")
    assert data == {"status": "OK", "results": []}

    mock_resp.data = b""
    data_empty = sess._request_json("/v1/marketstatus/now")
    assert data_empty == {}
    sess.close()
