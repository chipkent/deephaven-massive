"""Tests for `MassiveConfig`."""

import dataclasses
import inspect
from typing import Any, cast

import pytest

import deephaven_massive as dm
from deephaven_massive import _query, _session
from deephaven_massive._config import MassiveConfig
from deephaven_massive._enums import AssetClass, Subscription

ENV = (
    "MASSIVE_API_KEY",
    "MASSIVE_S3_ACCESS_KEY_ID",
    "MASSIVE_S3_SECRET_ACCESS_KEY",
    "DEEPHAVEN_MASSIVE_NONCANONICAL_SYMBOLS",
    "DEEPHAVEN_MASSIVE_UNREADABLE_SYMBOLS",
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


def test_the_key_comes_from_massive_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MASSIVE_API_KEY", "massive")
    assert MassiveConfig().api_key == "massive"


@pytest.mark.parametrize(
    "variable",
    [
        "MASSIVE_API_KEY",
        "MASSIVE_S3_ACCESS_KEY_ID",
        "MASSIVE_S3_SECRET_ACCESS_KEY",
        "DEEPHAVEN_MASSIVE_NONCANONICAL_SYMBOLS",
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
    assert dict(base.ws_hosts) == {}
    assert base.ack_timeout == 5.0
    assert base.reconnect_backoff == 30.0
    assert base.max_reconnect_backoff == 120.0
    assert base.flat_file_symbol_threshold == 50
    assert base.max_concurrency == 8
    assert base.rest_basic_subscriptions == frozenset()
    assert base.delayed_subscriptions == frozenset()
    assert base.flush_interval == 0.05
    assert base.rest_base_url == "https://api.massive.com"
    assert base.s3_endpoint_url == "https://files.massive.com"
    assert base.s3_bucket == "flatfiles"
    assert base.noncanonical_symbols is dm.OnError.LOG
    assert base.unreadable_symbols is dm.OnError.LOG


def test_defaults_live_in_exactly_one_place() -> None:
    # Every field has its default on the dataclass, the session takes only a config, and no
    # other module of the package repeats a default value.
    for f in dataclasses.fields(MassiveConfig):
        missing = dataclasses.MISSING
        assert f.default is not missing or f.default_factory is not missing, f.name
    assert list(inspect.signature(dm.MassiveSession).parameters) == ["config"]
    others = inspect.getsource(_session) + inspect.getsource(_query)
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
    assert _replace(base, reconnect_backoff=15.0).reconnect_backoff == 15.0
    with pytest.raises(ValueError, match="reconnect_backoff"):
        _replace(base, reconnect_backoff=0)
    with pytest.raises(
        ValueError,
        match=r"reconnect_backoff must be at least 15 s, not 14\.99: Massive takes roughly 10 to "
        r"30 s to release a dropped connection",
    ):
        _replace(base, reconnect_backoff=14.99)
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
    hosts = {AssetClass.STOCKS: "fullmarket-business.massive.com"}
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


@pytest.mark.parametrize(
    "host", [" delayed.massive.com", "delayed.massive.com ", "Delayed.Massive.com"]
)
def test_ws_hosts_are_taken_exactly_as_written(host: str) -> None:
    # Never trimmed or lower-cased; the message names the hostname to write.
    with pytest.raises(ValueError, match=r"; write 'delayed\.massive\.com'$"):
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


def test_rest_basic_subscriptions_are_frozen_and_hashable(base: MassiveConfig) -> None:
    declared = {Subscription.OPTIONS, Subscription.CURRENCIES}
    cfg = _replace(base, rest_basic_subscriptions=declared)
    declared.add(Subscription.STOCKS)  # the configuration keeps its own copy
    assert cfg.rest_basic_subscriptions == frozenset(
        {Subscription.OPTIONS, Subscription.CURRENCIES}
    )
    assert isinstance(cfg.rest_basic_subscriptions, frozenset)
    same = _replace(base, rest_basic_subscriptions=frozenset(cfg.rest_basic_subscriptions))
    assert cfg == same
    assert hash(cfg) == hash(same)
    assert _replace(base, rest_basic_subscriptions={}.keys()).rest_basic_subscriptions == set()


def test_rest_basic_subscriptions_hold_subscriptions_only(base: MassiveConfig) -> None:
    with pytest.raises(
        TypeError,
        match=r"^rest_basic_subscriptions must be a set of dm\.Subscription, such as "
        r"\{dm\.Subscription\.STOCKS\}, not list$",
    ):
        _replace(base, rest_basic_subscriptions=[Subscription.STOCKS])
    with pytest.raises(
        TypeError,
        match=r"^each rest_basic_subscriptions member must be dm\.Subscription, "
        r"e\.g\. dm\.Subscription\.OPTIONS$",
    ):
        _replace(base, rest_basic_subscriptions={"options"})
    # An asset class names the subscription that covers it.
    with pytest.raises(TypeError, match=r"e\.g\. dm\.Subscription\.STOCKS$"):
        _replace(base, rest_basic_subscriptions={AssetClass.STOCKS})
    # Forex and crypto are one subscription, whichever way they are spelled.
    for currency in (AssetClass.FOREX, AssetClass.CRYPTO, "forex", "Crypto"):
        with pytest.raises(
            TypeError,
            match=r"^rest_basic_subscriptions must hold dm\.Subscription members; forex and "
            r"crypto are one subscription, dm\.Subscription\.CURRENCIES$",
        ):
            _replace(base, rest_basic_subscriptions={currency})


def test_delayed_subscriptions_are_frozen_and_hashable(base: MassiveConfig) -> None:
    listed = {Subscription.OPTIONS, Subscription.FUTURES}
    cfg = _replace(base, delayed_subscriptions=listed)
    listed.add(Subscription.STOCKS)  # the configuration keeps its own copy
    assert cfg.delayed_subscriptions == frozenset({Subscription.OPTIONS, Subscription.FUTURES})
    assert isinstance(cfg.delayed_subscriptions, frozenset)
    same = _replace(base, delayed_subscriptions=frozenset(cfg.delayed_subscriptions))
    assert cfg == same
    assert hash(cfg) == hash(same)
    assert cfg != base


def test_delayed_subscriptions_hold_subscriptions_only(base: MassiveConfig) -> None:
    with pytest.raises(
        TypeError, match=r"^delayed_subscriptions must be a set of dm\.Subscription, such as "
    ):
        _replace(base, delayed_subscriptions=[Subscription.STOCKS])
    with pytest.raises(TypeError, match=r"^each delayed_subscriptions member must be"):
        _replace(base, delayed_subscriptions={"options"})
    with pytest.raises(TypeError, match=r"forex and crypto are one subscription"):
        _replace(base, delayed_subscriptions={"forex"})


def test_currencies_cannot_be_delayed(base: MassiveConfig) -> None:
    with pytest.raises(
        ValueError,
        match=r"^delayed_subscriptions cannot hold dm\.Subscription\.CURRENCIES: Massive has no "
        r"delayed forex or crypto feed",
    ):
        _replace(base, delayed_subscriptions={Subscription.CURRENCIES})


def test_a_delayed_pin_needs_the_subscription_listed(base: MassiveConfig) -> None:
    with pytest.raises(
        ValueError,
        match=r"^ws_hosts pins options to the delayed feed delayed\.massive\.com, but "
        r"delayed_subscriptions does not list dm\.Subscription\.OPTIONS; to use the delayed "
        r"feed write dm\.MassiveConfig\(delayed_subscriptions=\{dm\.Subscription\.OPTIONS\}\)$",
    ):
        _replace(base, ws_hosts={AssetClass.OPTIONS: "delayed.massive.com"})
    # Any host named as a delayed feed asks for delayed data.
    with pytest.raises(ValueError, match=r"^ws_hosts pins stocks to the delayed feed delayed\.x"):
        _replace(base, ws_hosts={AssetClass.STOCKS: "delayed.x.example.com"})


@pytest.mark.parametrize("asset_class", [AssetClass.FOREX, AssetClass.CRYPTO])
def test_forex_and_crypto_cannot_pin_a_delayed_feed(
    base: MassiveConfig, asset_class: AssetClass
) -> None:
    with pytest.raises(
        ValueError, match=r"Massive has no delayed forex or crypto feed; remove the pin$"
    ):
        _replace(base, ws_hosts={asset_class: "delayed.massive.com"})


def test_a_listed_subscription_cannot_pin_another_host(base: MassiveConfig) -> None:
    with pytest.raises(
        ValueError,
        match=r"^delayed_subscriptions lists dm\.Subscription\.STOCKS, whose streams use "
        r"Massive's delayed feed delayed\.massive\.com, but ws_hosts pins stocks to "
        r"socket\.massive\.com; remove one of the two$",
    ):
        _replace(
            base,
            delayed_subscriptions={Subscription.STOCKS},
            ws_hosts={AssetClass.STOCKS: "socket.massive.com"},
        )


def test_a_pin_that_agrees_is_accepted(base: MassiveConfig) -> None:
    cfg = _replace(
        base,
        delayed_subscriptions={Subscription.STOCKS},
        ws_hosts={AssetClass.STOCKS: "delayed.massive.com", AssetClass.OPTIONS: "o.example.com"},
    )
    assert cfg.ws_hosts[AssetClass.STOCKS] == "delayed.massive.com"


_SYMBOL_SETTINGS = [
    ("noncanonical_symbols", "DEEPHAVEN_MASSIVE_NONCANONICAL_SYMBOLS"),
    ("unreadable_symbols", "DEEPHAVEN_MASSIVE_UNREADABLE_SYMBOLS"),
]


@pytest.mark.parametrize(("setting", "variable"), _SYMBOL_SETTINGS)
@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("raise", dm.OnError.RAISE),
        ("RAISE", dm.OnError.RAISE),
        ("log", dm.OnError.LOG),
        ("Ignore", dm.OnError.IGNORE),
    ],
)
def test_symbol_settings_from_the_environment(
    monkeypatch: pytest.MonkeyPatch, *, setting: str, variable: str, text: str, expected: dm.OnError
) -> None:
    monkeypatch.setenv(variable, text)
    config = MassiveConfig(api_key="k")
    assert getattr(config, setting) is expected
    other = next(name for name, _ in _SYMBOL_SETTINGS if name != setting)
    assert getattr(config, other) is dm.OnError.LOG  # each variable sets only its own setting


@pytest.mark.parametrize(("setting", "variable"), _SYMBOL_SETTINGS)
def test_a_symbol_setting_out_of_the_environment_is_refused(
    monkeypatch: pytest.MonkeyPatch, setting: str, variable: str
) -> None:
    del setting
    monkeypatch.setenv(variable, "warn")
    with pytest.raises(
        ValueError, match=rf"^{variable} must be 'raise', 'log', or 'ignore', not 'warn'$"
    ):
        MassiveConfig(api_key="k")


@pytest.mark.parametrize(("setting", "variable"), _SYMBOL_SETTINGS)
def test_symbol_settings_are_the_enum(base: MassiveConfig, setting: str, variable: str) -> None:
    del variable
    assert getattr(_replace(base, **{setting: dm.OnError.IGNORE}), setting) is dm.OnError.IGNORE
    with pytest.raises(
        TypeError, match=rf"^{setting} must be dm\.OnError, e\.g\. dm\.OnError\.LOG$"
    ):
        _replace(base, **{setting: "log"})


def test_api_key_must_be_a_string() -> None:
    with pytest.raises(TypeError, match="api_key must be a str, not NoneType"):
        MassiveConfig(api_key=cast("str", None))


def test_configs_are_hashable_and_compare_by_value() -> None:
    hosts = {AssetClass.STOCKS: "fullmarket-business.massive.com"}
    a = MassiveConfig(api_key="k", ws_hosts=hosts)
    b = MassiveConfig(api_key="k", ws_hosts=dict(hosts))
    assert a == b
    assert hash(a) == hash(b)
    assert len({a, b, MassiveConfig(api_key="k")}) == 2
    assert repr(a.ws_hosts) == repr(hosts)
    assert len(a.ws_hosts) == 1
    assert list(a.ws_hosts) == [AssetClass.STOCKS]


def test_equal_backoffs_are_accepted() -> None:
    cfg = MassiveConfig(api_key="k", reconnect_backoff=30.0, max_reconnect_backoff=30.0)
    assert cfg.max_reconnect_backoff == cfg.reconnect_backoff == 30.0
