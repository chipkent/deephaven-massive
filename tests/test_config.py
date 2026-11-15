"""Tests for `MassiveConfig`."""

import dataclasses
import inspect
import logging
import os
import re
import subprocess
import sys
import sysconfig
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

import deephaven_massive as dm
from deephaven_massive import _config, _session
from deephaven_massive._config import CONFIG_FILE_VARIABLE, MassiveConfig
from deephaven_massive._enums import AssetClass, OnError, Subscription
from deephaven_massive.queries import _call, _choice, _fetch, _history, _query, _stream

ENV = (
    "MASSIVE_API_KEY",
    "MASSIVE_S3_ACCESS_KEY_ID",
    "MASSIVE_S3_SECRET_ACCESS_KEY",
    "DEEPHAVEN_MASSIVE_CONFIG_FILE",
)


@pytest.fixture(autouse=True)
def clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ENV:
        monkeypatch.delenv(name, raising=False)


@pytest.fixture
def base() -> MassiveConfig:
    return MassiveConfig(api_key="k")


def test_the_key_comes_from_massive_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MASSIVE_API_KEY", "massive")
    assert MassiveConfig().api_key == "massive"


@pytest.mark.parametrize(
    "variable",
    [
        "MASSIVE_API_KEY",
        "MASSIVE_S3_ACCESS_KEY_ID",
        "MASSIVE_S3_SECRET_ACCESS_KEY",
    ],
)
def test_an_environment_value_with_surrounding_whitespace_is_refused(
    monkeypatch: pytest.MonkeyPatch, variable: str
) -> None:
    # Never trimmed; the message names the variable but never the value, which may be a key.
    monkeypatch.setenv("MASSIVE_API_KEY", "k")
    monkeypatch.setenv(variable, " secret-value ")
    with pytest.raises(ValueError, match=f"^{variable} has leading or trailing whitespace") as e:
        MassiveConfig()
    assert "secret-value" not in str(e.value)


@pytest.mark.parametrize("key", [" massive", "massive\n"])
def test_an_api_key_with_surrounding_whitespace_is_refused(key: str) -> None:
    with pytest.raises(ValueError, match=r"^api_key has leading or trailing whitespace") as e:
        MassiveConfig(api_key=key)
    assert "massive" not in str(e.value)


@pytest.mark.parametrize("value", ["", "  ", " AKIA9", "AKIA9 "])
@pytest.mark.parametrize("name", ["s3_access_key_id", "s3_secret_access_key"])
def test_an_s3_credential_must_be_exact(name: str, value: str) -> None:
    both: dict[str, Any] = {"s3_access_key_id": "AKIA9", "s3_secret_access_key": "AKIA9"}
    both[name] = value
    with pytest.raises(ValueError, match=f"^{name} is blank or has leading or trailing") as e:
        MassiveConfig(api_key="k", **both)
    assert "AKIA9" not in str(e.value)


def test_polygon_api_key_is_not_read(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POLYGON_API_KEY", "poly")
    with pytest.raises(dm.AuthenticationError, match="MASSIVE_API_KEY"):
        MassiveConfig()


def test_blank_environment_values_are_absent(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MASSIVE_API_KEY", "  ")
    with pytest.raises(dm.AuthenticationError):
        MassiveConfig()


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
    assert dict(base.websocket_hosts) == {}
    assert base.subscription_confirmation_timeout == 5.0
    assert base.reconnect_backoff == 30.0
    assert base.max_reconnect_backoff == 120.0
    assert base.max_outage == 300.0
    assert base.flat_file_symbol_threshold == 50
    assert base.max_concurrency == 8
    assert base.rest_basic_subscriptions == frozenset()
    assert base.delayed_subscriptions == frozenset()
    assert base.flush_interval == 0.05
    assert base.rest_base_url == "https://api.massive.com"
    assert base.s3_endpoint_url == "https://files.massive.com"
    assert base.s3_bucket == "flatfiles"
    assert base.unsendable_symbols is dm.OnError.LOG
    assert base.unlisted_symbols is dm.OnError.LOG
    assert base.unreadable_symbols is dm.OnError.LOG
    assert base.unavailable_symbols is dm.OnError.LOG
    assert base.malformed_rows is dm.OnError.LOG
    assert base.refused_venues is dm.OnError.RAISE
    assert base.connection_taken is dm.ConnectionTaken.FAIL


def test_defaults_live_in_exactly_one_place() -> None:
    # Every field has its default on the dataclass, the session takes only a config, and no
    # other module of the package repeats a default value.
    for f in dataclasses.fields(MassiveConfig):
        missing = dataclasses.MISSING
        assert f.default is not missing or f.default_factory is not missing, f.name
    assert list(inspect.signature(dm.MassiveSession).parameters) == ["config"]
    others = "".join(
        map(inspect.getsource, (_session, _call, _choice, _fetch, _history, _query, _stream))
    )
    # The bucket name is matched as a string literal: _massive.flatfiles contains it.
    literals = ("120.0", "time(11, 30)", "files.massive.com", "api.massive.com", '"flatfiles"')
    for literal in literals:
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


def test_reconnect_backoff_bounds(base: MassiveConfig) -> None:
    assert dataclasses.replace(base, reconnect_backoff=15.0).reconnect_backoff == 15.0
    with pytest.raises(ValueError, match="reconnect_backoff"):
        dataclasses.replace(base, reconnect_backoff=0)
    with pytest.raises(
        ValueError,
        match=r"^reconnect_backoff must be at least 15 s, not 14\.99, which limits reconnect "
        r"churn$",
    ):
        dataclasses.replace(base, reconnect_backoff=14.99)
    with pytest.raises(ValueError, match="max_reconnect_backoff must not be below"):
        dataclasses.replace(base, reconnect_backoff=30.0, max_reconnect_backoff=20.0)


def test_max_outage_may_be_zero_but_not_negative(base: MassiveConfig) -> None:
    # Zero means "fail at the first disconnect", so it is a valid limit; an int becomes a float.
    zero = dataclasses.replace(base, max_outage=0).max_outage
    assert (zero, type(zero)) == (0.0, float)
    for bad in (-1, float("inf"), float("nan")):
        with pytest.raises(
            ValueError,
            match=r"max_outage must be zero or more seconds, got .*; 0 fails live tables at the "
            "first disconnect",
        ):
            dataclasses.replace(base, max_outage=bad)
    with pytest.raises(ValueError, match="max_outage must be finite, got an int too large"):
        dataclasses.replace(base, max_outage=10**400)


@pytest.mark.parametrize("value", ["300", True, None])
def test_max_outage_is_a_number(base: MassiveConfig, value: object) -> None:
    with pytest.raises(TypeError, match="max_outage must be a number of seconds, not "):
        dataclasses.replace(base, max_outage=value)  # type: ignore[arg-type]  # the point: refused at run time


@pytest.mark.parametrize(
    ("name", "change"),
    [
        (
            "subscription_confirmation_timeout",
            lambda c: dataclasses.replace(c, subscription_confirmation_timeout=0.0),
        ),
        ("flush_interval", lambda c: dataclasses.replace(c, flush_interval=-1.0)),
        (
            "subscription_confirmation_timeout",
            lambda c: dataclasses.replace(c, subscription_confirmation_timeout=float("inf")),
        ),
        (
            "subscription_confirmation_timeout",
            lambda c: dataclasses.replace(c, subscription_confirmation_timeout=float("nan")),
        ),
        ("max_concurrency", lambda c: dataclasses.replace(c, max_concurrency=0)),
        (
            "flat_file_symbol_threshold",
            lambda c: dataclasses.replace(c, flat_file_symbol_threshold=-5),
        ),
    ],
    ids=[
        "timeout-zero",
        "flush-negative",
        "timeout-inf",
        "timeout-nan",
        "concurrency-zero",
        "threshold-negative",
    ],
)
def test_positive_numbers(
    base: MassiveConfig, name: str, change: Callable[[MassiveConfig], MassiveConfig]
) -> None:
    with pytest.raises(ValueError, match=f"{name} must be positive"):
        change(base)


@pytest.mark.parametrize("value", [60.0, 90.0])
def test_subscription_confirmation_timeout_is_below_the_no_ack_resend_interval(
    base: MassiveConfig, value: float
) -> None:
    with pytest.raises(
        ValueError, match=r"subscription_confirmation_timeout must be below 60 s, not \d+: "
    ):
        dataclasses.replace(base, subscription_confirmation_timeout=value)
    assert (
        dataclasses.replace(
            base, subscription_confirmation_timeout=59.0
        ).subscription_confirmation_timeout
        == 59.0
    )


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("subscription_confirmation_timeout", "5"),
        ("subscription_confirmation_timeout", True),
        ("max_concurrency", 2.5),
        ("max_concurrency", False),
    ],
)
def test_numeric_types(base: MassiveConfig, name: str, value: object) -> None:
    with pytest.raises(TypeError, match=f"{name} must be"):
        dataclasses.replace(base, **{name: value})  # type: ignore[arg-type]  # the point: refused at run time


def test_websocket_hosts_are_validated_and_frozen() -> None:
    hosts = {AssetClass.STOCKS: "fullmarket-business.massive.com"}
    cfg = MassiveConfig(api_key="k", websocket_hosts=hosts)
    assert dict(cfg.websocket_hosts) == {AssetClass.STOCKS: "fullmarket-business.massive.com"}
    hosts[AssetClass.OPTIONS] = "x.massive.com"
    assert AssetClass.OPTIONS not in cfg.websocket_hosts
    with pytest.raises(TypeError):
        cfg.websocket_hosts[AssetClass.OPTIONS] = "y"  # type: ignore[index]  # the point: read-only


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
def test_websocket_hosts_must_be_bare_hostnames(host: str) -> None:
    with pytest.raises(ValueError, match="bare hostname"):
        MassiveConfig(api_key="k", websocket_hosts={AssetClass.STOCKS: host})


@pytest.mark.parametrize(
    "host", [" delayed.massive.com", "delayed.massive.com ", "Delayed.Massive.com"]
)
def test_websocket_hosts_are_taken_exactly_as_written(host: str) -> None:
    # Never trimmed or lower-cased; the message names the hostname to write.
    with pytest.raises(ValueError, match=r"; write 'delayed\.massive\.com'$"):
        MassiveConfig(api_key="k", websocket_hosts={AssetClass.STOCKS: host})


def test_websocket_hosts_types(base: MassiveConfig) -> None:
    with pytest.raises(
        TypeError,
        match=r"^websocket_hosts key must be dm\.AssetClass, e\.g\. dm\.AssetClass\.STOCKS$",
    ):
        dataclasses.replace(base, websocket_hosts={"stocks": "delayed.massive.com"})  # type: ignore[dict-item]  # the point: refused at run time
    with pytest.raises(TypeError, match="must be a hostname str"):
        dataclasses.replace(base, websocket_hosts={AssetClass.STOCKS: 5})  # type: ignore[dict-item]  # the point: refused at run time
    with pytest.raises(TypeError, match="websocket_hosts must be a mapping"):
        dataclasses.replace(base, websocket_hosts=["delayed.massive.com"])  # type: ignore[arg-type]  # the point: refused at run time


def test_rest_basic_subscriptions_are_frozen_and_hashable(base: MassiveConfig) -> None:
    declared = {Subscription.OPTIONS, Subscription.CURRENCIES}
    cfg = dataclasses.replace(base, rest_basic_subscriptions=declared)
    declared.add(Subscription.STOCKS)  # the configuration keeps its own copy
    assert cfg.rest_basic_subscriptions == frozenset(
        {Subscription.OPTIONS, Subscription.CURRENCIES}
    )
    assert isinstance(cfg.rest_basic_subscriptions, frozenset)
    same = dataclasses.replace(
        base, rest_basic_subscriptions=frozenset(cfg.rest_basic_subscriptions)
    )
    assert cfg == same
    assert hash(cfg) == hash(same)
    assert (
        dataclasses.replace(base, rest_basic_subscriptions={}.keys()).rest_basic_subscriptions
        == set()
    )


def test_rest_basic_subscriptions_hold_subscriptions_only(base: MassiveConfig) -> None:
    with pytest.raises(
        TypeError,
        match=r"^rest_basic_subscriptions must be a set of dm\.Subscription, such as "
        r"\{dm\.Subscription\.STOCKS\}, not list$",
    ):
        dataclasses.replace(base, rest_basic_subscriptions=[Subscription.STOCKS])  # type: ignore[arg-type]  # the point: refused at run time
    with pytest.raises(
        TypeError,
        match=r"^each rest_basic_subscriptions member must be dm\.Subscription, "
        r"e\.g\. dm\.Subscription\.OPTIONS$",
    ):
        dataclasses.replace(base, rest_basic_subscriptions={"options"})  # type: ignore[arg-type]  # the point: refused at run time
    # An asset class names the subscription that covers it.
    with pytest.raises(TypeError, match=r"e\.g\. dm\.Subscription\.STOCKS$"):
        dataclasses.replace(base, rest_basic_subscriptions={AssetClass.STOCKS})  # type: ignore[arg-type]  # the point: refused at run time
    # Forex and crypto are one subscription, whichever way they are spelled.
    for currency in (AssetClass.FOREX, AssetClass.CRYPTO, "forex", "Crypto"):
        with pytest.raises(
            TypeError,
            match=r"^rest_basic_subscriptions must hold dm\.Subscription members; forex and "
            r"crypto are one subscription, dm\.Subscription\.CURRENCIES$",
        ):
            dataclasses.replace(base, rest_basic_subscriptions={currency})  # type: ignore[arg-type]  # the point: refused at run time


def test_delayed_subscriptions_are_frozen_and_hashable(base: MassiveConfig) -> None:
    listed = {Subscription.OPTIONS, Subscription.FUTURES}
    cfg = dataclasses.replace(base, delayed_subscriptions=listed)
    listed.add(Subscription.STOCKS)  # the configuration keeps its own copy
    assert cfg.delayed_subscriptions == frozenset({Subscription.OPTIONS, Subscription.FUTURES})
    assert isinstance(cfg.delayed_subscriptions, frozenset)
    same = dataclasses.replace(base, delayed_subscriptions=frozenset(cfg.delayed_subscriptions))
    assert cfg == same
    assert hash(cfg) == hash(same)
    assert cfg != base


def test_delayed_subscriptions_hold_subscriptions_only(base: MassiveConfig) -> None:
    with pytest.raises(
        TypeError, match=r"^delayed_subscriptions must be a set of dm\.Subscription, such as "
    ):
        dataclasses.replace(base, delayed_subscriptions=[Subscription.STOCKS])  # type: ignore[arg-type]  # the point: refused at run time
    with pytest.raises(TypeError, match=r"^each delayed_subscriptions member must be"):
        dataclasses.replace(base, delayed_subscriptions={"options"})  # type: ignore[arg-type]  # the point: refused at run time
    with pytest.raises(TypeError, match=r"forex and crypto are one subscription"):
        dataclasses.replace(base, delayed_subscriptions={"forex"})  # type: ignore[arg-type]  # the point: refused at run time


def test_currencies_cannot_be_delayed(base: MassiveConfig) -> None:
    with pytest.raises(
        ValueError,
        match=r"^delayed_subscriptions cannot hold dm\.Subscription\.CURRENCIES: Massive has no "
        r"delayed forex or crypto feed",
    ):
        dataclasses.replace(base, delayed_subscriptions={Subscription.CURRENCIES})


def test_a_delayed_pin_needs_the_subscription_listed(base: MassiveConfig) -> None:
    with pytest.raises(
        ValueError,
        match=r"^websocket_hosts pins options to the delayed feed delayed\.massive\.com, but "
        r"delayed_subscriptions does not list dm\.Subscription\.OPTIONS; to use the delayed "
        r"feed write dm\.MassiveConfig\(delayed_subscriptions=\{dm\.Subscription\.OPTIONS\}\)$",
    ):
        dataclasses.replace(base, websocket_hosts={AssetClass.OPTIONS: "delayed.massive.com"})
    # Any host named as a delayed feed asks for delayed data.
    with pytest.raises(
        ValueError, match=r"^websocket_hosts pins stocks to the delayed feed delayed\.x"
    ):
        dataclasses.replace(base, websocket_hosts={AssetClass.STOCKS: "delayed.x.example.com"})


@pytest.mark.parametrize("asset_class", [AssetClass.FOREX, AssetClass.CRYPTO])
def test_forex_and_crypto_cannot_pin_a_delayed_feed(
    base: MassiveConfig, asset_class: AssetClass
) -> None:
    with pytest.raises(
        ValueError, match=r"Massive has no delayed forex or crypto feed; remove the pin$"
    ):
        dataclasses.replace(base, websocket_hosts={asset_class: "delayed.massive.com"})


def test_a_listed_subscription_cannot_pin_another_host(base: MassiveConfig) -> None:
    with pytest.raises(
        ValueError,
        match=r"^delayed_subscriptions lists dm\.Subscription\.STOCKS, whose streams use "
        r"Massive's delayed feed delayed\.massive\.com, but websocket_hosts pins stocks to "
        r"socket\.massive\.com; remove one of the two$",
    ):
        dataclasses.replace(
            base,
            delayed_subscriptions={Subscription.STOCKS},
            websocket_hosts={AssetClass.STOCKS: "socket.massive.com"},
        )


def test_a_pin_that_agrees_is_accepted(base: MassiveConfig) -> None:
    cfg = dataclasses.replace(
        base,
        delayed_subscriptions={Subscription.STOCKS},
        websocket_hosts={
            AssetClass.STOCKS: "delayed.massive.com",
            AssetClass.OPTIONS: "o.example.com",
        },
    )
    assert cfg.websocket_hosts[AssetClass.STOCKS] == "delayed.massive.com"


_ON_ERROR_SETTERS: dict[str, Callable[[MassiveConfig, dm.OnError], MassiveConfig]] = {
    "unsendable_symbols": lambda c, v: dataclasses.replace(c, unsendable_symbols=v),
    "unlisted_symbols": lambda c, v: dataclasses.replace(c, unlisted_symbols=v),
    "unreadable_symbols": lambda c, v: dataclasses.replace(c, unreadable_symbols=v),
    "unavailable_symbols": lambda c, v: dataclasses.replace(c, unavailable_symbols=v),
    "malformed_rows": lambda c, v: dataclasses.replace(c, malformed_rows=v),
}
"""Each `OnError` setting, by name, with a function that sets it on a config."""

_ON_ERROR_SETTINGS = list(_ON_ERROR_SETTERS)

_EVERY_ON_ERROR_SETTER: dict[str, Callable[[MassiveConfig, dm.OnError], MassiveConfig]] = {
    **_ON_ERROR_SETTERS,
    "refused_venues": lambda c, v: dataclasses.replace(c, refused_venues=v),
}
"""The `OnError` settings above and `refused_venues`, whose default is `RAISE`."""


@pytest.mark.parametrize("setting", _ON_ERROR_SETTINGS)
def test_on_error_settings_default_to_log_and_read_no_environment(
    monkeypatch: pytest.MonkeyPatch, setting: str
) -> None:
    # The settings' old environment variables are gone: a value there changes nothing.
    monkeypatch.setenv(f"DEEPHAVEN_MASSIVE_{setting.upper()}", "raise")
    assert getattr(MassiveConfig(api_key="k"), setting) is dm.OnError.LOG


@pytest.mark.parametrize("setting", list(_EVERY_ON_ERROR_SETTER))
def test_on_error_settings_are_the_enum(base: MassiveConfig, setting: str) -> None:
    changed = _EVERY_ON_ERROR_SETTER[setting](base, dm.OnError.IGNORE)
    assert getattr(changed, setting) is dm.OnError.IGNORE
    with pytest.raises(
        TypeError, match=rf"^{setting} must be dm\.OnError, e\.g\. dm\.OnError\.LOG$"
    ):
        dataclasses.replace(base, **{setting: "log"})  # type: ignore[arg-type]  # the point: refused at run time


def test_connection_taken_is_the_enum(base: MassiveConfig) -> None:
    reconnect = dm.ConnectionTaken.RECONNECT
    assert dataclasses.replace(base, connection_taken=reconnect).connection_taken is reconnect
    with pytest.raises(
        TypeError,
        match=r"^connection_taken must be dm\.ConnectionTaken, e\.g\. "
        r"dm\.ConnectionTaken\.RECONNECT$",
    ):
        dataclasses.replace(base, connection_taken="reconnect")  # type: ignore[arg-type]  # the point: refused at run time


def test_api_key_must_be_a_string() -> None:
    with pytest.raises(TypeError, match="api_key must be a str, not NoneType"):
        MassiveConfig(api_key=None)  # type: ignore[arg-type]  # the point: refused at run time


@pytest.mark.parametrize("api_key", ["s3c r3t", "s3c\tr3t", "s3cr3t\r\nX: y", "s3cr\u00e9t"])
def test_an_api_key_no_header_can_carry_is_refused_at_the_config(api_key: str) -> None:
    with pytest.raises(ValueError, match=r"^api_key must be non-empty visible ASCII") as info:
        MassiveConfig(api_key=api_key)
    assert "s3c" not in str(info.value)


def test_configs_are_hashable_and_compare_by_value() -> None:
    hosts = {AssetClass.STOCKS: "fullmarket-business.massive.com"}
    a = MassiveConfig(api_key="k", websocket_hosts=hosts)
    b = MassiveConfig(api_key="k", websocket_hosts=dict(hosts))
    assert a == b
    assert hash(a) == hash(b)
    assert len({a, b, MassiveConfig(api_key="k")}) == 2
    assert repr(a.websocket_hosts) == repr(hosts)
    assert len(a.websocket_hosts) == 1
    assert list(a.websocket_hosts) == [AssetClass.STOCKS]


def test_equal_backoffs_are_accepted() -> None:
    cfg = MassiveConfig(api_key="k", reconnect_backoff=30.0, max_reconnect_backoff=30.0)
    assert cfg.max_reconnect_backoff == cfg.reconnect_backoff == 30.0


def test_the_repr_never_shows_the_user_and_password_of_a_url() -> None:
    config = dm.MassiveConfig(
        api_key="k",
        s3_endpoint_url="https://reader:hunter2@files.massive.com",
    )
    shown = repr(config)
    assert "hunter2" not in shown
    assert "s3_endpoint_url='https://***@files.massive.com'" in shown
    assert shown.startswith("MassiveConfig(s3_access_key_id=")
    assert "api_key" not in shown


_URL_SETTERS: dict[str, Callable[[MassiveConfig, str], MassiveConfig]] = {
    "rest_base_url": lambda c, url: dataclasses.replace(c, rest_base_url=url),
    "s3_endpoint_url": lambda c, url: dataclasses.replace(c, s3_endpoint_url=url),
}
"""Each URL setting, by name, with a function that sets it on a config."""


@pytest.mark.parametrize("name", list(_URL_SETTERS))
@pytest.mark.parametrize(
    "url",
    [
        "",
        "~",
        "api.massive.com",
        "ftp://api.massive.com",
        "https://",
        "https:///path",
        "https://api.massive.com/a b",
        " https://api.massive.com",
        "http://[::1",
        "https://u@",
        "HTTPS://api.massive.com",
    ],
)
def test_a_url_setting_must_be_an_http_url_with_a_host(name: str, url: str) -> None:
    with pytest.raises(ValueError, match=f"^{name} must be an http or https URL with a host"):
        _URL_SETTERS[name](MassiveConfig(api_key="k"), url)


@pytest.mark.parametrize(
    "url",
    ["http://127.0.0.1:8080", "http://localhost", "https://api.massive.com/", "http://[::1]:9000"],
)
def test_an_http_url_with_a_host_is_accepted(url: str) -> None:
    config = MassiveConfig(api_key="k", rest_base_url=url, s3_endpoint_url=url)
    assert (config.rest_base_url, config.s3_endpoint_url) == (url, url)


@pytest.mark.parametrize(
    "url",
    [
        "https://files.massive.com/v1?x=1",
        "https://reader:pw@files.massive.com",
        "http://minio:9000",
    ],
)
def test_the_s3_endpoint_may_hold_what_the_rest_base_url_may_not(url: str) -> None:
    assert MassiveConfig(api_key="k", s3_endpoint_url=url).s3_endpoint_url == url


@pytest.mark.parametrize(
    ("url", "reason"),
    [
        ("https://api.massive.com/v3", "must name only a scheme, host, and port"),
        ("https://api.massive.com?apiKey=1", "must name only a scheme, host, and port"),
        ("https://api.massive.com#top", "must name only a scheme, host, and port"),
        ("https://u:hunter2@api.massive.com", "must name only a scheme, host, and port"),
        ("http://api.massive.com", "would send the API key in clear text; use https"),
        ("http://10.0.0.1:8080", "would send the API key in clear text; use https"),
        ("https://api.massive.com:99999", "is not an absolute http or https URL"),
        ("https://api.massive.com:0", "is not an absolute http or https URL"),
    ],
)
def test_a_rest_base_url_the_client_would_refuse_is_refused_at_the_config(
    url: str, reason: str
) -> None:
    with pytest.raises(ValueError, match=f"^rest_base_url '.*' {reason}") as info:
        MassiveConfig(api_key="k", rest_base_url=url)
    assert "hunter2" not in str(info.value)


def test_a_refused_url_never_shows_a_password() -> None:
    with pytest.raises(ValueError, match="must be an http") as info:
        MassiveConfig(api_key="k", rest_base_url="ftp://u:hunter2@api.massive.com")
    assert "hunter2" not in str(info.value)
    assert "'ftp://***@api.massive.com'" in str(info.value)


@pytest.mark.parametrize("bucket", ["", "  "])
def test_the_bucket_must_not_be_blank(bucket: str) -> None:
    with pytest.raises(ValueError, match=r"^s3_bucket must name a bucket"):
        MassiveConfig(api_key="k", s3_bucket=bucket)


@pytest.mark.parametrize("name", ["rest_base_url", "s3_endpoint_url", "s3_bucket"])
def test_the_text_settings_must_be_strings(base: MassiveConfig, name: str) -> None:
    with pytest.raises(TypeError, match=f"^{name} must be a str, not NoneType"):
        dataclasses.replace(base, **{name: None})  # type: ignore[arg-type]  # the point: refused at run time


def test_a_refused_ws_host_never_shows_a_password() -> None:
    with pytest.raises(ValueError, match=r"got '\*\*\*@delayed\.massive\.com'$") as info:
        dm.MassiveConfig(
            api_key="k", websocket_hosts={dm.AssetClass.STOCKS: "u:hunter2@delayed.massive.com"}
        )
    assert "hunter2" not in str(info.value)


# --- The settings file -------------------------------------------------------------------------


def _settings(tmp_path: Path, text: str, name: str = "settings.yaml") -> Path:
    """Write a settings file.

    Returns:
        Its path.
    """
    file = tmp_path / name
    file.write_text(text, encoding="utf-8")
    return file


def _refused(tmp_path: Path, text: str) -> str:
    """Read a file holding `text`, which `from_file` must refuse.

    Returns:
        The refusal's message after its leading `settings file <path>`.
    """
    file = _settings(tmp_path, text)
    with pytest.raises(ValueError, match=f"^settings file {re.escape(str(file))}") as info:
        MassiveConfig.from_file(file)
    return str(info.value).removeprefix(f"settings file {file}")


@pytest.fixture
def key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MASSIVE_API_KEY", "k")


def test_the_converter_table_covers_every_field_but_the_credentials() -> None:
    names = {f.name for f in dataclasses.fields(MassiveConfig)}
    assert set(_config._CONVERTERS).isdisjoint(_config._CREDENTIAL_VARIABLES)
    assert set(_config._CONVERTERS) | set(_config._CREDENTIAL_VARIABLES) == names


@pytest.mark.usefixtures("key")
def test_a_file_sets_every_field_once(tmp_path: Path) -> None:
    file = _settings(
        tmp_path,
        """\
# Every setting a file may hold.
websocket_hosts:
  options: delayed.massive.com
  stocks: fullmarket-business.massive.com
subscription_confirmation_timeout: 4.5
reconnect_backoff: 20
max_reconnect_backoff: 1e2
max_outage: 0
flat_file_symbol_threshold: 10
max_concurrency: 4
rest_basic_subscriptions: [stocks, currencies]
delayed_subscriptions:
  - options
flush_interval: 0.25
rest_base_url: https://rest.example.com
s3_endpoint_url: "https://files.example.com"
s3_bucket: 'bucket'
unsendable_symbols: raise
unlisted_symbols: raise
unreadable_symbols: ignore
unavailable_symbols: raise
malformed_rows: ignore
refused_venues: log
connection_taken: reconnect
""",
    )
    expected = MassiveConfig(
        api_key="k",
        websocket_hosts={
            AssetClass.OPTIONS: "delayed.massive.com",
            AssetClass.STOCKS: "fullmarket-business.massive.com",
        },
        subscription_confirmation_timeout=4.5,
        reconnect_backoff=20.0,
        max_reconnect_backoff=100.0,
        max_outage=0.0,
        flat_file_symbol_threshold=10,
        max_concurrency=4,
        rest_basic_subscriptions={Subscription.STOCKS, Subscription.CURRENCIES},
        delayed_subscriptions={Subscription.OPTIONS},
        flush_interval=0.25,
        rest_base_url="https://rest.example.com",
        s3_endpoint_url="https://files.example.com",
        s3_bucket="bucket",
        unsendable_symbols=OnError.RAISE,
        unlisted_symbols=OnError.RAISE,
        unreadable_symbols=OnError.IGNORE,
        unavailable_symbols=OnError.RAISE,
        malformed_rows=OnError.IGNORE,
        refused_venues=OnError.LOG,
        connection_taken=dm.ConnectionTaken.RECONNECT,
    )
    config = MassiveConfig.from_file(file)
    assert config == expected
    assert type(config.max_concurrency) is int
    assert type(config.reconnect_backoff) is float


@pytest.mark.usefixtures("key")
def test_a_setting_the_file_leaves_out_keeps_its_default_and_code_wins(tmp_path: Path) -> None:
    config = MassiveConfig.from_file(_settings(tmp_path, "max_outage: 600\nmax_concurrency: 2\n"))
    assert config == MassiveConfig(api_key="k", max_outage=600.0, max_concurrency=2)
    assert dataclasses.replace(config, max_concurrency=6).max_concurrency == 6


@pytest.mark.usefixtures("key")
def test_an_empty_mapping_sets_nothing(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    file = _settings(tmp_path, "{}\n")
    with caplog.at_level(logging.INFO, logger="deephaven_massive._config"):
        assert MassiveConfig.from_file(file) == MassiveConfig(api_key="k")
    assert caplog.messages == [f"settings file {file} set no settings"]


@pytest.mark.usefixtures("key")
def test_from_file_logs_the_file_and_the_keys_it_set(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    file = _settings(tmp_path, "max_outage: 600\ns3_bucket: b\n")
    with caplog.at_level(logging.INFO, logger="deephaven_massive._config"):
        MassiveConfig.from_file(file)
    assert caplog.messages == [f"settings file {file} set max_outage, s3_bucket"]


@pytest.mark.usefixtures("key")
def test_the_path_comes_from_the_environment_variable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    file = _settings(tmp_path, "max_concurrency: 3\n")
    monkeypatch.setenv(CONFIG_FILE_VARIABLE, str(file))
    with caplog.at_level(logging.INFO, logger="deephaven_massive._config"):
        assert MassiveConfig.from_file().max_concurrency == 3
    assert caplog.messages == [
        f"settings file {file} (from DEEPHAVEN_MASSIVE_CONFIG_FILE) set max_concurrency"
    ]


def test_no_path_and_no_variable_is_refused_naming_the_variable() -> None:
    with pytest.raises(
        ValueError,
        match=r"^no settings file: pass its path to MassiveConfig\.from_file, or set "
        r"DEEPHAVEN_MASSIVE_CONFIG_FILE to it$",
    ):
        MassiveConfig.from_file()


def test_neither_the_constructor_nor_a_session_reads_the_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(CONFIG_FILE_VARIABLE, str(_settings(tmp_path, "max_concurrency: 3\n")))
    assert MassiveConfig(api_key="k").max_concurrency == 8


@pytest.mark.usefixtures("key")
@pytest.mark.parametrize("through_variable", [False, True])
def test_a_leading_tilde_is_the_home_folder(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, through_variable: bool
) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))
    _settings(tmp_path, "max_concurrency: 5\n")
    if through_variable:
        monkeypatch.setenv(CONFIG_FILE_VARIABLE, "~/settings.yaml")
        assert MassiveConfig.from_file().max_concurrency == 5
    else:
        assert MassiveConfig.from_file("~/settings.yaml").max_concurrency == 5


def test_a_missing_file_is_named(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    missing = tmp_path / "absent.yaml"
    with pytest.raises(
        FileNotFoundError, match=f"^settings file {re.escape(str(missing))} does not exist$"
    ):
        MassiveConfig.from_file(str(missing))
    monkeypatch.setenv(CONFIG_FILE_VARIABLE, str(missing))
    with pytest.raises(
        FileNotFoundError,
        match=rf"^settings file {re.escape(str(missing))} does not exist "
        r"\(from DEEPHAVEN_MASSIVE_CONFIG_FILE\)$",
    ):
        MassiveConfig.from_file()


def test_a_refusal_is_logged_at_debug(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    missing = tmp_path / "absent.yaml"
    with (
        caplog.at_level(logging.DEBUG, logger="deephaven_massive._config"),
        pytest.raises(FileNotFoundError),
    ):
        MassiveConfig.from_file(missing)
    assert caplog.messages == [
        f"MassiveConfig.from_file({missing!r}) refused: settings file {missing} does not exist"
    ]


def test_a_file_that_is_not_utf8_is_refused(tmp_path: Path) -> None:
    file = tmp_path / "settings.yaml"
    file.write_bytes(b"s3_bucket: caf\xe9\n")
    with pytest.raises(
        ValueError,
        match=f"^settings file {re.escape(str(file))} is not UTF-8 text: invalid continuation byte",
    ):
        MassiveConfig.from_file(file)


def test_a_control_character_is_refused(tmp_path: Path) -> None:
    assert "unacceptable character #x0007" in _refused(tmp_path, "s3_bucket: a\x07b\n")


def test_a_syntax_error_names_its_line_and_column(tmp_path: Path) -> None:
    message = _refused(tmp_path, "max_outage: 600\ndelayed_subscriptions: [options\n")
    assert message == ", line 3, column 1: expected ',' or ']', but got '<stream end>'"


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        (
            "",
            (
                ": the top level must be a mapping of setting names to values, such as "
                "'max_outage: 600'"
            ),
        ),
        (
            "# only a comment\n",
            (
                ": the top level must be a mapping of setting names to "
                "values, such as 'max_outage: 600'"
            ),
        ),
        ("- max_outage\n", ", line 1, column 1: the top level must be a mapping"),
        ("max_outage\n", ", line 1, column 1: the top level must be a mapping"),
    ],
)
def test_the_top_level_must_be_a_mapping(tmp_path: Path, text: str, expected: str) -> None:
    assert _refused(tmp_path, text).startswith(expected)


def test_a_second_document_is_refused(tmp_path: Path) -> None:
    message = _refused(tmp_path, "max_outage: 600\n---\nmax_concurrency: 4\n")
    assert message == ", line 2, column 1: a second document starts here; a settings file holds one"


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        (
            "s3_bucket: !!python/object:os.system ls\n",
            ", line 1, column 12: a tag (tag:yaml.org,2002:python/object:os.system) is not allowed",
        ),
        ("s3_bucket: !!str ON\n", ", line 1, column 12: a tag (tag:yaml.org,2002:str) is not"),
        ("s3_bucket: !custom ON\n", ", line 1, column 12: a tag (!custom) is not allowed"),
        ("delayed_subscriptions: !!seq [options]\n", ", line 1, column 24: a tag"),
        ("websocket_hosts: !!map {stocks: a.example.com}\n", ", line 1, column 18: a tag"),
        ("!!python/name:os.system max_outage: 1\n", ", line 1, column 1: a tag"),
    ],
)
def test_a_tag_is_refused(tmp_path: Path, text: str, expected: str) -> None:
    assert _refused(tmp_path, text).startswith(expected)


def test_an_anchor_and_an_alias_are_refused(tmp_path: Path) -> None:
    message = _refused(
        tmp_path, "max_outage: &seconds 300\nsubscription_confirmation_timeout: *seconds\n"
    )
    assert message == (
        ", line 1, column 13: an anchor (&seconds) is not allowed; write each value out"
    )
    message = _refused(tmp_path, "max_outage: 300\nsubscription_confirmation_timeout: *seconds\n")
    assert message == ", line 2, column 36: an alias (*name) is not allowed; write the value out"


def test_deep_nesting_is_refused_at_its_line_instead_of_overflowing_the_stack(
    tmp_path: Path,
) -> None:
    # Thousands of levels would exhaust Python's recursion limit inside PyYAML's composer.
    flow = "max_outage: " + "[" * 5000 + "]" * 5000 + "\n"
    assert _refused(tmp_path, flow) == (
        ", line 1, column 22: lists and mappings nest more than 10 deep here; a setting needs "
        "two levels at most"
    )
    block = "".join(f"{'  ' * level}k:\n" for level in range(3000))
    assert _refused(tmp_path, block).startswith(", line 11, column 21: lists and mappings nest")
    # Ten levels, the top mapping's included, are composed and refused by the setting itself.
    shallow = "max_outage: " + "[" * 9 + "]" * 9 + "\n"
    assert _refused(tmp_path, shallow).startswith(", line 1, column 13: max_outage must be")


def test_a_duplicate_key_is_refused(tmp_path: Path) -> None:
    message = _refused(tmp_path, "max_outage: 300\nmax_concurrency: 2\nmax_outage: 600\n")
    assert message == (
        ", line 3, column 1: max_outage is set twice; set it once (it is first set on line 1)"
    )


def test_a_key_that_is_not_text_is_refused(tmp_path: Path) -> None:
    message = _refused(tmp_path, "[max_outage]: 300\n")
    assert message == ", line 1, column 1: must be a setting name, not a list or mapping"


@pytest.mark.parametrize(
    ("name", "variable"),
    [
        ("api_key", "MASSIVE_API_KEY"),
        ("s3_access_key_id", "MASSIVE_S3_ACCESS_KEY_ID"),
        ("s3_secret_access_key", "MASSIVE_S3_SECRET_ACCESS_KEY"),
    ],
)
def test_a_credential_is_refused_without_showing_it(
    tmp_path: Path, name: str, variable: str
) -> None:
    message = _refused(tmp_path, f"max_outage: 300\n{name}: hunter2-secret\n")
    assert message == (
        f", line 2, column 1: {name} is a credential, which a settings file may not hold; "
        f"remove it and set {variable} in the environment, or pass it in code (the value is "
        "not shown)"
    )


def test_an_unknown_key_suggests_the_closest(tmp_path: Path) -> None:
    message = _refused(tmp_path, "max_outages: 300\n")
    assert (
        message == ", line 1, column 1: unknown setting 'max_outages'; did you mean 'max_outage'?"
    )
    # A pasted secret as a key shows only its first 40 characters.
    secret = "a" * 40 + "hunter2-tail"
    message = _refused(tmp_path, f"{secret}: 1\n")
    assert message.startswith(f", line 1, column 1: unknown setting '{'a' * 40}…'; the settings")
    assert "hunter2" not in message
    message = _refused(tmp_path, "zzz: 1\n")
    assert message == (
        ", line 1, column 1: unknown setting 'zzz'; the settings are "
        + ", ".join(_config._CONVERTERS)
    )


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        # The error settings: an exact word.
        (
            "unsendable_symbols: Raise\n",
            (
                ", line 1, column 21: unsendable_symbols 'Raise' "
                "is not an error setting; write one of raise, log, ignore, exactly as shown"
            ),
        ),
        (
            'unreadable_symbols: " raise"\n',
            (
                ", line 1, column 21: unreadable_symbols ' raise' "
                "is not an error setting; write one of raise, log, ignore, exactly as shown"
            ),
        ),
        (
            "connection_taken: Reconnect\n",
            (
                ", line 1, column 19: connection_taken 'Reconnect' "
                "is not a connection_taken value; write one of fail, reconnect, exactly as shown"
            ),
        ),
        (
            "unreadable_symbols: [log]\n",
            (
                ", line 1, column 21: unreadable_symbols must be an "
                "error setting (raise, log, ignore), not a list or mapping"
            ),
        ),
        # The subscription lists.
        (
            "delayed_subscriptions: options\n",
            (
                ", line 1, column 24: delayed_subscriptions must "
                "be a list of subscriptions, such as [stocks, options], or [] for none"
            ),
        ),
        (
            "delayed_subscriptions: [Options]\n",
            (
                ", line 1, column 25: delayed_subscriptions 'Options' is not a subscription; "
                "write one of stocks, options, indices, currencies, futures, exactly as shown"
            ),
        ),
        (
            "rest_basic_subscriptions: [forex]\n",
            ", line 1, column 28: rest_basic_subscriptions 'forex' is not a subscription",
        ),
        (
            "rest_basic_subscriptions: [ON]\n",
            ", line 1, column 28: rest_basic_subscriptions 'ON' is not a subscription",
        ),
        (
            "rest_basic_subscriptions: [stocks, stocks]\n",
            ", line 1, column 36: rest_basic_subscriptions lists 'stocks' twice",
        ),
        (
            "rest_basic_subscriptions: [[stocks]]\n",
            ", line 1, column 28: rest_basic_subscriptions must be a subscription",
        ),
        # The durations.
        (
            "max_outage: soon\n",
            (
                ", line 1, column 13: max_outage 'soon' is not a finite "
                "number of seconds; write one such as 300 or 0.5"
            ),
        ),
        ("max_outage: .inf\n", ", line 1, column 13: max_outage '.inf' is not a finite"),
        ("max_outage: nan\n", ", line 1, column 13: max_outage 'nan' is not a finite"),
        ("max_outage: 1e999\n", ", line 1, column 13: max_outage '1e999' is not a finite"),
        (
            "subscription_confirmation_timeout: 0700\n",
            ", line 1, column 36: subscription_confirmation_timeout '0700' is not a finite",
        ),
        ("flush_interval: 1_0\n", ", line 1, column 17: flush_interval '1_0' is not a finite"),
        ("flush_interval: \n", ", line 1, column 16: flush_interval '' is not a finite"),
        (
            "max_outage: {a: 1}\n",
            ", line 1, column 13: max_outage must be a number of seconds, not a list or mapping",
        ),
        # The counts.
        (
            "max_concurrency: 8.0\n",
            ", line 1, column 18: max_concurrency '8.0' is not a whole number; write one such as 8",
        ),
        ("max_concurrency: 010\n", ", line 1, column 18: max_concurrency '010' is not a whole"),
        (
            "flat_file_symbol_threshold: fifty\n",
            ", line 1, column 29: flat_file_symbol_threshold 'fifty' is not a whole number",
        ),
        # The host mapping.
        (
            "websocket_hosts: delayed.massive.com\n",
            (
                ", line 1, column 18: websocket_hosts must be a mapping of "
                "asset class to hostname, such as {stocks: delayed.massive.com}"
            ),
        ),
        (
            "websocket_hosts: {Stocks: a.example.com}\n",
            (
                ", line 1, column 19: websocket_hosts 'Stocks' is not "
                "an asset class; write one of stocks, options, futures, indices, forex, crypto"
            ),
        ),
        (
            "websocket_hosts:\n  stocks: a.example.com\n  stocks: b.example.com\n",
            ", line 3, column 3: websocket_hosts pins 'stocks' twice",
        ),
        (
            "websocket_hosts: {stocks: [a.example.com]}\n",
            ", line 1, column 27: websocket_hosts must be a hostname, not a list or mapping",
        ),
        # The counts: more digits than Python converts, without echoing them.
        (
            "max_concurrency: " + "9" * 5000 + "\n",
            (
                ", line 1, column 18: max_concurrency is a number of 5000 characters, too "
                "long to read; write one such as 8"
            ),
        ),
        # The text settings.
        *(
            (
                text,
                (
                    f", line 1, column {column}: {name} is empty; write the value, or leave "
                    "the setting out for its default"
                ),
            )
            for text, name, column in [
                ("rest_base_url:\n", "rest_base_url", 15),
                ("s3_endpoint_url: ~\n", "s3_endpoint_url", 18),
                ("s3_bucket: null\n", "s3_bucket", 12),
                ("s3_bucket: NULL\n", "s3_bucket", 12),
                ("s3_bucket: ''\n", "s3_bucket", 12),
            ]
        ),
        (
            "rest_base_url: [https://api.massive.com]\n",
            ", line 1, column 16: rest_base_url must be text, not a list or mapping",
        ),
    ],
)
def test_each_converter_refuses_what_it_cannot_convert(
    tmp_path: Path, text: str, expected: str
) -> None:
    assert _refused(tmp_path, text).startswith(expected)


@pytest.mark.usefixtures("key")
def test_tickers_like_on_and_no_stay_text(tmp_path: Path) -> None:
    # YAML 1.1 reads ON, NO, Y, and off as booleans, and 0700 as octal; the file keeps text.
    file = _settings(
        tmp_path,
        "s3_bucket: NO\nwebsocket_hosts: {stocks: y}\n",
    )
    config = MassiveConfig.from_file(file)
    assert config.s3_bucket == "NO"
    assert config.websocket_hosts[AssetClass.STOCKS] == "y"


@pytest.mark.usefixtures("key")
def test_the_constructor_still_validates_and_its_message_names_the_line(tmp_path: Path) -> None:
    message = _refused(tmp_path, "max_concurrency: 2\nmax_outage: -1\n")
    assert message == (
        ", line 2, column 1: max_outage must be zero or more seconds, got -1.0; 0 fails live "
        "tables at the first disconnect"
    )
    message = _refused(tmp_path, "delayed_subscriptions: [currencies]\n")
    assert message.startswith(
        ", line 1, column 1: delayed_subscriptions cannot hold dm.Subscription.CURRENCIES"
    )
    message = _refused(tmp_path, "websocket_hosts: {stocks: Socket.massive.com}\n")
    assert message.startswith(
        ", line 1, column 1: websocket_hosts[stocks]='Socket.massive.com' must"
    )
    message = _refused(tmp_path, "max_outage: 600\nrest_base_url: ON\n")
    assert message.startswith(", line 2, column 1: rest_base_url must be an http or https URL")
    message = _refused(tmp_path, "max_outage: 600\nrest_base_url: https://api.massive.com/v3\n")
    assert message.startswith(
        ", line 2, column 1: rest_base_url 'https://api.massive.com/v3' must name only a scheme"
    )
    message = _refused(tmp_path, "s3_bucket: '~'\ns3_endpoint_url: '0700'\n")
    assert message.startswith(", line 2, column 1: s3_endpoint_url must be an http or https URL")
    message = _refused(tmp_path, "reconnect_backoff: 200\n")
    assert message == ": max_reconnect_backoff must not be below reconnect_backoff"


@pytest.mark.parametrize(
    "variable",
    ["MASSIVE_API_KEY", "MASSIVE_S3_ACCESS_KEY_ID", "MASSIVE_S3_SECRET_ACCESS_KEY"],
)
def test_an_environment_problem_names_the_variable_and_not_the_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, variable: str
) -> None:
    monkeypatch.setenv("MASSIVE_API_KEY", "k")
    monkeypatch.setenv(variable, " padded ")
    file = _settings(tmp_path, "max_outage: 600\n")
    with pytest.raises(ValueError, match=f"^{variable} has leading or trailing whitespace"):
        MassiveConfig.from_file(file)
    monkeypatch.setenv("MASSIVE_API_KEY", "k")
    monkeypatch.delenv("MASSIVE_S3_SECRET_ACCESS_KEY", raising=False)
    monkeypatch.setenv("MASSIVE_S3_ACCESS_KEY_ID", "AKIA9")
    with pytest.raises(ValueError, match=r"^flat files need both s3_access_key_id"):
        MassiveConfig.from_file(file)


def test_a_missing_key_is_still_an_authentication_error(tmp_path: Path) -> None:
    with pytest.raises(dm.AuthenticationError, match="no Massive API key") as missing:
        MassiveConfig.from_file(_settings(tmp_path, "max_outage: 600\n"))
    # Massive refused nothing: the settings lack the key, and that is no bug.
    assert (missing.value.source, missing.value.is_bug) == (dm.ErrorSource.USER, False)


@pytest.mark.usefixtures("key")
@pytest.mark.skipif(not sysconfig.get_config_var("Py_GIL_DISABLED"), reason="a build with the GIL")
def test_reading_a_file_keeps_the_gil_disabled(tmp_path: Path) -> None:
    # The suite runs with PYTHON_GIL=0, which keeps the GIL off whatever an extension declares;
    # a fresh interpreter without it shows whether PyYAML's import would turn the GIL back on.
    environment = {name: value for name, value in os.environ.items() if name != "PYTHON_GIL"}
    script = (
        "import sys, yaml\n"
        "assert not sys._is_gil_enabled(), 'importing yaml enabled the GIL'\n"
        "yaml.compose('max_outage: 600', Loader=yaml.BaseLoader)\n"
        "assert not sys._is_gil_enabled(), 'composing a document enabled the GIL'\n"
    )
    result = subprocess.run(  # noqa: S603 - this interpreter, with a fixed script
        [sys.executable, "-W", "error", "-c", script],
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    MassiveConfig.from_file(_settings(tmp_path, "max_outage: 600\n"))
    assert not getattr(sys, "_is_gil_enabled", lambda: True)()


def test_a_refused_rest_base_url_never_shows_its_query() -> None:
    with pytest.raises(ValueError, match="must name only") as info:
        MassiveConfig(api_key="k", rest_base_url="https://api.massive.com?apiKey=hunter2")
    assert "'https://api.massive.com?***'" in str(info.value)
    assert "hunter2" not in str(info.value)


@pytest.mark.parametrize(
    ("url", "shown"),
    [
        ("ftp://api.massive.com?apiKey=hunter2", "'ftp://api.massive.com?***'"),
        ("https://api.massive.com?apiKey=hun ter2", "'https://api.massive.com?***'"),
        ("ftp://u:pw@api.massive.com#hunter2", "'ftp://***@api.massive.com#***'"),
    ],
)
def test_an_early_rest_base_url_refusal_never_shows_its_query(
    url: str, shown: str, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG, logger="deephaven_massive")
    with pytest.raises(ValueError, match=r"^rest_base_url must be an http") as info:
        MassiveConfig(api_key="k", rest_base_url=url)
    assert str(info.value).endswith(f"not {shown}")
    assert "hunter2" not in str(info.value)
    assert "hunter2" not in caplog.text


def test_an_s3_endpoint_refusal_still_shows_its_query() -> None:
    with pytest.raises(ValueError, match=r"not 'ftp://files\.massive\.com\?x=1'$"):
        MassiveConfig(api_key="k", s3_endpoint_url="ftp://files.massive.com?x=1")


# --- The settings reference --------------------------------------------------------------------

CONFIGURATION_PAGE = Path(__file__).resolve().parent.parent / "docs" / "configuration.md"

HOST_PATHS = [f"websocket_hosts.{asset_class.value}" for asset_class in AssetClass]
"""The full path of each `websocket_hosts` key, as the page's table names it."""


def test_the_committed_settings_page_is_the_rendered_one() -> None:
    page = CONFIGURATION_PAGE.read_text(encoding="utf-8")
    assert page == _config.with_settings_reference(page), (
        "docs/configuration.md is out of date; run `uv run python scripts/render_reference.py`"
    )


@pytest.mark.usefixtures("key")
def test_the_example_file_sets_every_setting_to_its_example_value(tmp_path: Path) -> None:
    text = _config._example_file()
    config = MassiveConfig.from_file(_settings(tmp_path, text))
    # Every setting a file may hold is written, so a new setting can't be left out of the page.
    assert set(re.findall(r"^(\w+):", text, re.MULTILINE)) == set(_config._CONVERTERS)
    assert {name: getattr(config, name) for name in _config._EXAMPLE} == {
        name: example.value for name, example in _config._EXAMPLE.items()
    }


def test_the_example_file_comments_each_setting_and_marks_its_defaults() -> None:
    text = _config._example_file()
    assert text.startswith(
        "# Plans that stream 15 minutes late: Stocks Starter and Options Developer.\n"
        "delayed_subscriptions: [options, stocks]\n\n"
    )
    assert "\nmax_outage: 600\n" in text
    assert "\nmax_concurrency: 8  # the default\n" in text
    assert "\nwebsocket_hosts:\n  indices: business.massive.com\n" in text
    settings = re.findall(r"^\w+:", text, re.MULTILINE)
    assert len(re.findall(r"^# ", text, re.MULTILINE)) == len(settings)
    assert all(len(line) <= 100 for line in text.splitlines())


def test_the_table_is_one_row_per_setting_and_websocket_hosts_key_in_order() -> None:
    reference = _config.settings_reference()
    rows = [line.split("`")[1] for line in reference.splitlines() if line.startswith("| `")]
    expected: list[str] = []
    for name in _config._EXAMPLE:
        expected += HOST_PATHS if name == "websocket_hosts" else [name]
    assert rows == expected
    outage = "A number of seconds, `0` or more, such as `600`"
    texts = _config._setting_texts()
    assert f"| `max_outage` | {texts['max_outage']} | {outage} | `300` |" in reference
    assert "| `flush_interval` |" in reference
    assert "`0.05` |" in reference
    assert "| `log`, `ignore`, or `raise` | `log` |" in reference
    assert "api_key" not in reference


def test_each_websocket_hosts_row_gives_its_rule_and_the_host_used_without_a_pin() -> None:
    reference = _config.settings_reference()
    assert (
        "| `websocket_hosts.forex` | Sets the WebSocket host forex streams come from. | A "
        "lower-case hostname, such as `socket.massive.com`; never a `delayed.` host | "
        "`socket.massive.com` |"
    ) in reference
    assert (
        "| `websocket_hosts.stocks` | Sets the WebSocket host stocks streams come from. | A "
        "lower-case hostname, such as `socket.massive.com`; a `delayed.` host exactly when "
        "`delayed_subscriptions` lists `stocks` | `delayed.massive.com` when "
        "`delayed_subscriptions` lists `stocks`, else `socket.massive.com` |"
    ) in reference
    assert (
        "| `websocket_hosts.indices` | Sets the WebSocket host indices streams come from. | A "
        "lower-case hostname, such as `business.massive.com`;"
    ) in reference


def test_each_setting_has_its_detail_in_a_settings_files_spelling() -> None:
    reference = _config.settings_reference()
    details = reference[reference.index('<a id="setting-details"></a>') :]
    headings = [line[5:] for line in details.splitlines() if line.startswith("#### ")]
    assert headings == list(_config._EXAMPLE)
    # A list in the docstring stays a list, its members spelled as a file writes them.
    assert "\n- `raise` raises `ValueError` for a bad row at the call, and fails a live\n" in (
        details
    )
    assert "Never list\n`currencies`, the one subscription for forex and crypto" in details
    # A full link to another page under docs/ becomes a relative link.
    assert "see\n[Report a bug](troubleshooting.md#report-a-bug)." in details
    assert "https://github.com" not in details
    assert "dm.OnError" not in reference
    assert "dm.Subscription" not in reference


@pytest.mark.usefixtures("key")
def test_every_value_forms_example_is_accepted_by_a_settings_file(tmp_path: Path) -> None:
    forms = _config._value_forms()
    assert set(forms) == set(_config._CONVERTERS) - {"websocket_hosts"} | set(HOST_PATHS)
    for name, form in forms.items():
        example = re.search(r"such as `([^`]+)`", form) or re.search(r"`([^`]+)`$", form)
        assert example is not None, form
        setting, _, key = name.partition(".")
        line = f"{setting}:\n  {key}: {example[1]}\n" if key else f"{name}: {example[1]}\n"
        # Raises if the settings file refuses the example.
        MassiveConfig.from_file(_settings(tmp_path, line))


def test_delayed_subscriptions_offers_only_what_it_accepts() -> None:
    form = _config._value_forms()["delayed_subscriptions"]
    assert "`currencies`" not in form
    assert "`currencies`" in _config._value_forms()["rest_basic_subscriptions"]


def test_the_reference_takes_each_entry_from_the_docstring_without_its_default() -> None:
    texts = _config._setting_texts()
    assert set(texts) == {f.name for f in dataclasses.fields(MassiveConfig)}
    # The default comes from the code, so the entry's last sentence giving it is left out, and
    # the paragraphs after the first are the setting's detail.
    assert texts["s3_bucket"] == "Sets the S3 bucket flat files are read from."
    assert "Defaults to" not in texts["reconnect_backoff"]
    assert "EntitlementError" not in texts["delayed_subscriptions"]
    assert texts["unavailable_symbols"] == (
        "Decides what happens to a symbol Massive answers as not found, or one a `poll()` can't "
        "request with the arguments you gave it."
    )
    details = _config._setting_details()
    assert details["s3_bucket"] == ["Change it only with `s3_endpoint_url`."]
    assert "EntitlementError" in details["delayed_subscriptions"][1]
    assert details["unsendable_symbols"][1].startswith(
        "- `dm.OnError.LOG` skips the row and logs a warning naming the value.\n"
        "- `dm.OnError.IGNORE` does the same with no warning: only the `DEBUG` log names it.\n"
    )


@pytest.mark.parametrize(
    ("value", "written"),
    [
        (OnError.RAISE, "raise"),
        (frozenset({Subscription.STOCKS, Subscription.OPTIONS}), "[options, stocks]"),
        (frozenset(), "[]"),
        ({AssetClass.STOCKS: "socket.massive.com"}, "{stocks: socket.massive.com}"),
        ({}, "{}"),
        (0.05, "0.05"),
        (300.0, "300"),
        (8, "8"),
        ("flatfiles", "flatfiles"),
    ],
)
def test_a_value_is_written_as_a_settings_file_writes_it(value: object, written: str) -> None:
    assert _config._file_value(value) == written


def test_the_reference_refuses_an_example_that_misses_or_adds_a_setting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    example = dict(_config._EXAMPLE)
    first = next(iter(example))
    monkeypatch.setattr(_config, "_EXAMPLE", {k: v for k, v in example.items() if k != first})
    with pytest.raises(ValueError, match="not every file setting once"):
        _config.settings_reference()
    monkeypatch.setattr(_config, "_EXAMPLE", {**example, "api_key": example[first]})
    with pytest.raises(ValueError, match="not every file setting once"):
        _config.settings_reference()


def test_only_the_generated_part_of_the_page_is_rewritten() -> None:
    start, end = _config.SETTINGS_REFERENCE_START, _config.SETTINGS_REFERENCE_END
    page = f"# Hand\n\nIntro.\n\n{start}\nstale\n{end}\nAfter.\n"
    rewritten = _config.with_settings_reference(page)
    assert rewritten.startswith(f'# Hand\n\nIntro.\n\n{start}\n\n<a id="example-settings-file">')
    assert rewritten.endswith(f"\n{end}\nAfter.\n")
    assert "stale" not in rewritten
    assert _config.with_settings_reference(rewritten) == rewritten


@pytest.mark.parametrize(
    "page",
    [
        "no markers",
        f"{_config.SETTINGS_REFERENCE_START} only the start",
        f"{_config.SETTINGS_REFERENCE_END} then {_config.SETTINGS_REFERENCE_START}",
    ],
)
def test_a_page_without_both_markers_is_refused(page: str) -> None:
    with pytest.raises(ValueError, match="SETTINGS_REFERENCE_START and SETTINGS_REFERENCE_END"):
        _config.with_settings_reference(page)


def test_every_url_setting_is_shown_without_its_credentials() -> None:
    # A URL may hold a user and password; a new URL setting must say how it is shown.
    urls = {f.name for f in dataclasses.fields(MassiveConfig) if f.name.endswith("_url")}
    assert urls == set(_config._URL_SETTINGS)
