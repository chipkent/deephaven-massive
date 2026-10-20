"""Tests for MassiveConfig, MassiveEntitlementError, and MassiveSession."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from unittest.mock import MagicMock

import pytest

from deephaven_massive.config import MassiveConfig, MassiveEntitlementError
from deephaven_massive.session import MassiveSession, ensure_deephaven_server


def test_config_from_args_and_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
    monkeypatch.delenv("POLYGON_API_KEY", raising=False)
    monkeypatch.delenv("MASSIVE_S3_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("MASSIVE_S3_SECRET_ACCESS_KEY", raising=False)

    with pytest.raises(ValueError, match="Massive API key is required"):
        MassiveConfig.from_args(api_key=None)

    with pytest.raises(ValueError, match="Massive API key is required"):
        MassiveConfig.from_args(api_key="   ")

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

    with pytest.raises(ValueError, match="flush_interval_ms must be > 0"):
        MassiveConfig.from_args(api_key="k", flush_interval_ms=0)

    with pytest.raises(ValueError, match="max_batch_size must be > 0"):
        MassiveConfig.from_args(api_key="k", max_batch_size=0)

    with pytest.raises(ValueError, match="max_retries must be >= 0"):
        MassiveConfig.from_args(api_key="k", max_retries=-1)


def test_config_resolve_ws_url() -> None:
    cfg = MassiveConfig.from_args(api_key="k", feed="realtime")
    assert cfg.resolve_ws_url("stocks") == "wss://socket.massive.com/stocks"
    assert (
        cfg.resolve_ws_url("options", feed_override="delayed")
        == "wss://delayed.massive.com/options"
    )
    assert (
        cfg.resolve_ws_url("stocks", is_fmv=True)
        == "wss://business.massive.com/stocks"
    )
    assert (
        cfg.resolve_ws_url("stocks", feed_override="delayed", is_fmv=True)
        == "wss://delayed-business.massive.com/stocks"
    )
    assert (
        cfg.resolve_ws_url("futures", venue="cme")
        == "wss://socket.massive.com/futures/cme"
    )
    assert (
        cfg.resolve_ws_url("stocks", feed_override="custom.host.example.com")
        == "wss://custom.host.example.com/stocks"
    )
    with pytest.raises(ValueError, match="Unsupported futures venue"):
        cfg.resolve_ws_url("futures", venue="invalid_venue")


def test_ensure_deephaven_server_branches(monkeypatch: pytest.MonkeyPatch) -> None:
    from deephaven_server import Server

    orig_instance = Server.instance
    try:
        monkeypatch.setattr(Server, "instance", None)
        assert ensure_deephaven_server(auto_start=False) is None

        started = []

        class DummyServer:
            def __init__(self, port: int, jvm_args: list[str]) -> None:
                self.port = port
                self.jvm_args = jvm_args

            def start(self) -> None:
                started.append(self.port)
                Server.instance = orig_instance

        monkeypatch.setattr("deephaven_server.Server", DummyServer)
        monkeypatch.setattr(DummyServer, "instance", None, raising=False)
        ctx = ensure_deephaven_server(auto_start=True, port=10099)
        assert started == [10099]
        assert ctx is not None
    finally:
        Server.instance = orig_instance


def test_session_request_json_and_retries() -> None:
    calls: list[str] = []
    sleeps: list[float] = []

    def _handler(
        url: str,
        params: Mapping[str, Any] | None,
        headers: Mapping[str, str],
    ) -> tuple[int, Any]:
        calls.append(url)
        assert headers["Authorization"] == "Bearer test-key"
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
        sleep_fn=sleeps.append,
    ) as sess:
        res = sess.request_json("v3/trades/AAPL", {"limit": 10, "empty": None})
        assert res["results"] == [{"ok": True}]
        assert calls[-1].endswith("/v3/trades/AAPL?limit=10")

        # Params dict where all values are None
        res_all_none = sess.request_json("/v3/trades/AAPL", {"empty": None})
        assert res_all_none["status"] == "OK"

        # Duplicate register_builder is idempotent
        b = sess.stocks.trades("AAPL")
        sess.register_builder(b)
        sess.register_builder(b)

        # Full URL with existing query string
        res2 = sess.request_json(
            "https://api.massive.com/v3/trades/AAPL?cursor=abc",
            {"limit": 5},
        )
        assert res2["status"] == "OK"
        assert "cursor=abc&limit=5" in calls[-1]

        # Flaky endpoint succeeds after retry
        calls.clear()
        res_flaky = sess.request_json("/flaky")
        assert res_flaky["status"] == "OK"
        assert len(calls) == 3
        assert len(sleeps) == 2

        # Entitlement 403 error
        with pytest.raises(MassiveEntitlementError, match="Upgrade plan"):
            sess.request_json("/entitlement_error")

        # Non-retryable 400 error
        with pytest.raises(RuntimeError, match="HTTP 400"):
            sess.request_json("/hard_error")


def test_session_urllib3_pool_execution() -> None:
    sess = MassiveSession(api_key="test-key")
    mock_resp = MagicMock()
    mock_resp.status = 200
    mock_resp.data = b'{"status": "OK", "results": []}'
    sess._http_pool = MagicMock()
    sess._http_pool.request.return_value = mock_resp

    data = sess.request_json("/v1/marketstatus/now")
    assert data == {"status": "OK", "results": []}

    mock_resp.data = b""
    data_empty = sess.request_json("/v1/marketstatus/now")
    assert data_empty == {}
    sess.close()
