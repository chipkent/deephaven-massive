"""Tests for `MassiveConfig` and the JSON codec choice (DESIGN § 5, § 23 item 12)."""

import dataclasses
import inspect
import json
import logging
import threading
from datetime import time
from types import ModuleType, SimpleNamespace
from typing import Any, cast

import pytest

import deephaven_massive as dm
from deephaven_massive import _config, _query, _session
from deephaven_massive._config import (
    MassiveConfig,
    _select_json_codec,
    json_codec,
    log_json_codec_once,
)
from deephaven_massive._dates import NEW_YORK
from deephaven_massive._enums import AssetClass

ENV = (
    "MASSIVE_API_KEY",
    "POLYGON_API_KEY",
    "MASSIVE_S3_ACCESS_KEY_ID",
    "MASSIVE_S3_SECRET_ACCESS_KEY",
)


@pytest.fixture(autouse=True)
def clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ENV:
        monkeypatch.delenv(name, raising=False)


def _replace(config: MassiveConfig, **changes: object) -> MassiveConfig:
    # dataclasses.replace with untyped values, to exercise the run-time validation.
    return dataclasses.replace(config, **cast("dict[str, Any]", changes))


@pytest.fixture
def base() -> MassiveConfig:
    return MassiveConfig(api_key="k")


def test_massive_key_wins_over_polygon_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POLYGON_API_KEY", "poly")
    assert MassiveConfig().api_key == "poly"
    monkeypatch.setenv("MASSIVE_API_KEY", "massive")
    assert MassiveConfig().api_key == "massive"


def test_empty_environment_values_are_absent(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MASSIVE_API_KEY", "  ")
    monkeypatch.setenv("POLYGON_API_KEY", "poly")
    assert MassiveConfig().api_key == "poly"


def test_missing_key_raises_authentication_error() -> None:
    with pytest.raises(dm.AuthenticationError, match="MASSIVE_API_KEY"):
        MassiveConfig()
    with pytest.raises(dm.AuthenticationError):
        MassiveConfig(api_key=" ")


def test_s3_credentials_from_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MASSIVE_S3_ACCESS_KEY_ID", "id")
    monkeypatch.setenv("MASSIVE_S3_SECRET_ACCESS_KEY", "secret")
    cfg = MassiveConfig(api_key="k")
    assert (cfg.s3_access_key_id, cfg.s3_secret_access_key) == ("id", "secret")


def test_partial_s3_credentials_are_rejected() -> None:
    with pytest.raises(ValueError, match="both s3_access_key_id and s3_secret_access_key"):
        MassiveConfig(api_key="k", s3_access_key_id="id")


def test_defaults(base: MassiveConfig) -> None:
    assert base.s3_access_key_id is None
    assert base.s3_secret_access_key is None
    assert dict(base.ws_hosts) == {}
    assert base.ack_timeout == 5.0
    assert base.reconnect_backoff == 15.0
    assert base.max_reconnect_backoff == 120.0
    assert base.flat_file_symbol_threshold == 50
    assert base.flat_file_publish_cutoff == time(11, 30)
    assert base.max_concurrency == 8
    assert base.flush_interval == 0.05
    assert base.rest_base_url == "https://api.massive.com"
    assert base.s3_endpoint_url == "https://files.massive.com"
    assert base.s3_bucket == "flatfiles"


def test_defaults_live_in_exactly_one_place() -> None:
    # Every field has its default on the dataclass, the session takes only a config, and no
    # other module of the package repeats a default value.
    for f in dataclasses.fields(MassiveConfig):
        missing = dataclasses.MISSING
        assert f.default is not missing or f.default_factory is not missing, f.name
    assert list(inspect.signature(dm.MassiveSession).parameters) == ["config"]
    others = inspect.getsource(_session) + inspect.getsource(_query)
    for literal in ("120.0", "time(11, 30)", "files.massive.com", "api.massive.com", "flatfiles"):
        assert literal not in others


def test_frozen_and_secrets_hidden() -> None:
    hidden_s3, hidden_key = "s3-hidden-value", "api-hidden-value"
    cfg = MassiveConfig(api_key=hidden_key, s3_access_key_id="id", s3_secret_access_key=hidden_s3)
    with pytest.raises(dataclasses.FrozenInstanceError):
        cfg.__setattr__("api_key", "x")
    text = repr(cfg)
    assert hidden_key not in text
    assert hidden_s3 not in text
    assert "s3_access_key_id='id'" in text


def test_reconnect_backoff_floor(base: MassiveConfig) -> None:
    with pytest.raises(ValueError, match="at least 15 seconds"):
        _replace(base, reconnect_backoff=14.9)
    assert _replace(base, reconnect_backoff=15).reconnect_backoff == 15
    with pytest.raises(ValueError, match="max_reconnect_backoff must not be below"):
        _replace(base, reconnect_backoff=30.0, max_reconnect_backoff=20.0)


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("ack_timeout", 0.0),
        ("flush_interval", -1.0),
        ("ack_timeout", float("inf")),
        ("ack_timeout", float("nan")),
        ("max_concurrency", 0),
        ("flat_file_symbol_threshold", -5),
    ],
)
def test_positive_numbers(base: MassiveConfig, name: str, value: float) -> None:
    with pytest.raises(ValueError, match=f"{name} must be positive"):
        _replace(base, **{name: value})


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("ack_timeout", "5"),
        ("ack_timeout", True),
        ("max_concurrency", 2.5),
        ("max_concurrency", False),
    ],
)
def test_numeric_types(base: MassiveConfig, name: str, value: object) -> None:
    with pytest.raises(TypeError, match=f"{name} must be"):
        _replace(base, **{name: value})


def test_ws_hosts_are_validated_and_frozen() -> None:
    hosts = {AssetClass.STOCKS: " FullMarket-Business.massive.com "}
    cfg = MassiveConfig(api_key="k", ws_hosts=hosts)
    assert dict(cfg.ws_hosts) == {AssetClass.STOCKS: "fullmarket-business.massive.com"}
    hosts[AssetClass.OPTIONS] = "x.massive.com"
    assert AssetClass.OPTIONS not in cfg.ws_hosts
    with pytest.raises(TypeError):
        cast("dict[AssetClass, str]", cfg.ws_hosts)[AssetClass.OPTIONS] = "y"


@pytest.mark.parametrize(
    "host",
    [
        "wss://socket.massive.com",
        "socket.massive.com:443",
        "socket.massive.com/stocks",
        "",
        "-a.com",
    ],
)
def test_ws_hosts_must_be_bare_hostnames(host: str) -> None:
    with pytest.raises(ValueError, match="bare hostname"):
        MassiveConfig(api_key="k", ws_hosts={AssetClass.STOCKS: host})


def test_ws_hosts_types(base: MassiveConfig) -> None:
    with pytest.raises(
        TypeError, match=r"^ws_hosts key must be dm\.AssetClass, e\.g\. dm\.AssetClass\.STOCKS$"
    ):
        _replace(base, ws_hosts={"stocks": "delayed.massive.com"})
    with pytest.raises(TypeError, match="must be a hostname str"):
        _replace(base, ws_hosts={AssetClass.STOCKS: 5})
    with pytest.raises(TypeError, match="ws_hosts must be a mapping"):
        _replace(base, ws_hosts=["delayed.massive.com"])


def test_publish_cutoff_is_a_wall_clock_time(base: MassiveConfig) -> None:
    with pytest.raises(ValueError, match="drop tzinfo"):
        _replace(base, flat_file_publish_cutoff=time(11, 30, tzinfo=NEW_YORK))


def test_json_codec_with_and_without_orjson() -> None:
    def missing(name: str) -> ModuleType:
        raise ImportError(name)

    assert _select_json_codec(missing) == ("json", json.loads)

    fake = cast("ModuleType", SimpleNamespace(loads=json.loads))
    name, loads = _select_json_codec(lambda _name: fake)
    assert name == "orjson"
    assert loads(b'{"a": 1}') == {"a": 1}
    assert json_codec()[0] in {"json", "orjson"}


def test_json_codec_is_logged_once(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(_config, "_codec_logged", threading.Event())
    with caplog.at_level(logging.INFO, logger="deephaven_massive._config"):
        log_json_codec_once()
        log_json_codec_once()
    assert [r.getMessage() for r in caplog.records] == [f"JSON codec: {json_codec()[0]}"]
