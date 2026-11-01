"""`MassiveConfig`, the session's immutable configuration, and the settings file that can fill it.

`MassiveConfig` is the only way to configure the library: `MassiveSession(MassiveConfig(...))`.
Every default lives in exactly one place, the field defaults of the dataclass below; nothing else in
the package repeats them. `MassiveConfig()` with no arguments reads the API key and the S3
credentials from the environment, and nothing else, and validates everything at construction, so
a bad configuration fails before any session exists.

`MassiveConfig.from_file` builds a configuration from a YAML settings file, so a notebook, a
console, and a container can share one set of settings. This module owns that reader too: a
PyYAML `BaseLoader` subclass that keeps every scalar as text and refuses YAML's riskier features
(tags, anchors and aliases, duplicate keys, several documents), and one table, `_CONVERTERS`, that
turns each setting's text into the value its field takes. The constructor stays the only
validator of values. Nothing reads a file unless asked: there is no default location.

What is deliberately *not* configurable: the options contract cap (1,000 per connection), the
evidence endpoints of the host decision, the plan tier (Massive offers no entitlement endpoint,
so the library learns the tier from REST evidence instead), the REST rate of a Basic
subscription (5 requests per minute, a wire constant; `rest_basic_subscriptions` only names the
subscriptions on Basic), and the JSON codec. The wire
package owns the codec: it uses `orjson` when the optional extra `deephaven-massive[orjson]` is
installed and the standard library otherwise, and logs the choice once at INFO.
"""

from __future__ import annotations

import difflib
import logging
import math
import os
import re
from collections.abc import Callable, Mapping
from collections.abc import Set as AbstractSet
from dataclasses import dataclass, field, fields
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final, Protocol, TypeVar

import yaml

from deephaven_massive._enums import (
    AssetClass,
    OnError,
    Subscription,
    _require_enum,
    subscription_of,
)
from deephaven_massive._errors import AuthenticationError
from deephaven_massive._massive import without_userinfo
from deephaven_massive._types import override

if TYPE_CHECKING:
    from collections.abc import Iterator

__all__ = [
    "BUSINESS_HOST",
    "CONFIG_FILE_VARIABLE",
    "DELAYED_HOST",
    "DELAYED_PREFIX",
    "MIN_RECONNECT_BACKOFF",
    "NO_ACK_RESEND",
    "REALTIME_HOST",
    "HostMap",
    "MassiveConfig",
    "delayed_fix",
    "environment_text",
    "require_positive_int",
    "require_positive_number",
    "shown_setting",
]

logger = logging.getLogger(__name__)

CONFIG_FILE_VARIABLE: Final = "DEEPHAVEN_MASSIVE_CONFIG_FILE"
"""The environment variable `MassiveConfig.from_file()` reads the settings file's path from."""

MIN_RECONNECT_BACKOFF: Final = 15.0
"""The shortest `reconnect_backoff` accepted, in seconds.

Massive releases a dropped connection only after roughly 10 to 30 seconds; a client that
retries sooner can be refused for holding the allowance it just lost.
"""

NO_ACK_RESEND: Final = 60.0
"""Seconds between rounds of re-sends of the subscriptions Massive has not acknowledged.

A round sends every such topic in one subscribe frame on the open socket, so it costs the other
streams nothing; one a minute stays well above the 15 s Massive asks between reconnects. Each
round awaits its answer for `ack_timeout`, which must therefore be shorter.
"""

REALTIME_HOST: Final = "socket.massive.com"
"""Massive's real-time WebSocket feed, and the one documented host of forex and crypto."""

DELAYED_HOST: Final = "delayed.massive.com"
"""Massive's delayed WebSocket feed: 15 minutes behind, 10 for futures."""

BUSINESS_HOST: Final = "business.massive.com"
"""The WebSocket host of the Business plans' fair-market-value (FMV) channels, real-time only."""

_HOSTNAME: Final = re.compile(
    r"^(?=.{1,253}$)[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?(\.[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?)*$"
)


def environment_text(name: str) -> str | None:
    """Return an environment variable's value exactly as set, or None when it is absent.

    The library takes text exactly as written and never trims it, so a value with surrounding
    whitespace (a key pasted with a trailing space, say) is refused rather than cleaned.

    Args:
        name: The variable's name, such as `"MASSIVE_API_KEY"`.

    Returns:
        The value, or None when the variable is unset or blank.

    Raises:
        ValueError: If the value has leading or trailing whitespace. The message names the
            variable but never its value, which may be a credential.
    """
    value = os.environ.get(name, "")
    if not value.strip():
        return None
    if value != value.strip():
        raise ValueError(
            f"{name} has leading or trailing whitespace; set it without any (its value is "
            "not shown, since it may be a secret)"
        )
    return value


def _env_api_key() -> str:
    """Read the API key from `MASSIVE_API_KEY`, Massive's own name for it.

    Returns:
        The key, or an empty string when it is unset (rejected by validation).
    """
    return environment_text("MASSIVE_API_KEY") or ""


def _env_s3_access_key_id() -> str | None:
    """Read `MASSIVE_S3_ACCESS_KEY_ID`.

    Returns:
        The S3 access key id, or None.
    """
    return environment_text("MASSIVE_S3_ACCESS_KEY_ID")


def _env_s3_secret_access_key() -> str | None:
    """Read `MASSIVE_S3_SECRET_ACCESS_KEY`.

    Returns:
        The S3 secret access key, or None.
    """
    return environment_text("MASSIVE_S3_SECRET_ACCESS_KEY")


@dataclass(frozen=True, kw_only=True, slots=True)
class MassiveConfig:
    """Everything a `MassiveSession` needs to know: credentials, plan, hosts, timeouts, limits.

    Construct it with keyword arguments; any field left out takes its default, and the
    credentials default to the environment. The object is frozen and validated at
    construction, so it can be shared between sessions and threads. Secrets are left out of
    its `repr`.

    Example:
        ::

            import deephaven_massive as dm

            cfg = dm.MassiveConfig()  # MASSIVE_API_KEY from the environment
            # Real-time stocks; options from Massive's delayed feed.
            mixed = dm.MassiveConfig(delayed_subscriptions={dm.Subscription.OPTIONS})
            pinned = dm.MassiveConfig(
                ws_hosts={dm.AssetClass.STOCKS: "fullmarket-business.massive.com"},
                max_concurrency=4,
            )
            m = dm.MassiveSession(pinned)

    `MassiveConfig.from_file()` builds one from a settings file instead; see there.

    Attributes:
        api_key: The Massive API key. Defaults to the `MASSIVE_API_KEY` environment variable;
            with neither, construction raises `AuthenticationError`.
        s3_access_key_id: The flat-file S3 access key id. Defaults to the
            `MASSIVE_S3_ACCESS_KEY_ID` environment variable, else None. Flat-file history
            needs both S3 credentials; without them, `history()` cannot read flat files.
        s3_secret_access_key: The flat-file S3 secret. Defaults to the
            `MASSIVE_S3_SECRET_ACCESS_KEY` environment variable, else None.
        delayed_subscriptions: The subscriptions whose live data you accept from Massive's
            delayed feed, 15 minutes behind (10 for futures), such as
            `{dm.Subscription.OPTIONS}`. A listed subscription's streams read the delayed feed.
            An unlisted one is real-time: a stream, or a `fetch()` or `poll()` of current data,
            raises `EntitlementError` naming the line to add when Massive shows the data is
            delayed. Past data and `history()` are never checked. `dm.Subscription.CURRENCIES`
            cannot be listed and is never refused; FMV streams are real-time only; a `ws_hosts`
            pin must match. Defaults to an empty set. See README.md#real-time-or-delayed.
        rest_basic_subscriptions: The Massive subscriptions this key's account holds on the
            free Basic tier, such as `{dm.Subscription.OPTIONS}`. Massive limits REST requests
            per subscription: Basic allows 5 requests per minute, and every paid tier has no
            limit. The REST requests of a declared subscription are paced at 5 per minute
            from the first one, shared with every other session of the process that uses the
            same key. Any other subscription is unpaced until Massive refuses one of its
            requests with HTTP 429: then its requests pause and resume, and when the refusal
            says the per-minute maximum was exceeded, the library learns the subscription is
            Basic and paces it from then on. Forex and crypto are one subscription,
            `dm.Subscription.CURRENCIES`. `m.rate_limits()` shows each subscription's state.
            Defaults to an empty set: every subscription is unpaced until a refusal.
        ws_hosts: Pins the WebSocket hostname of the TICK channel family of an asset class,
            for example `{dm.AssetClass.STOCKS: "fullmarket-business.massive.com"}`, instead of
            letting the library decide from REST evidence. A pin must agree with
            `delayed_subscriptions` (see there). Hostnames only: no scheme, port, or path. The
            FMV family is never pinned; it is always `business.massive.com`. Defaults to
            an empty mapping: the library decides every host.
        ack_timeout: Seconds to wait for a subscription acknowledgement before re-sending it
            once. Must be below 60 seconds, the spacing of the later re-sends of
            unacknowledged subscriptions, each of which awaits its answer this long.
            Defaults to 5 seconds.
        reconnect_backoff: Seconds of the first wait before the WebSocket client tries again
            after any failure that is not fatal: a connection that opened and closed (cleanly
            or not, including a `max_connections` refusal), a TCP or TLS failure, a handshake
            or authentication timeout, or a retryable HTTP status during the upgrade (500,
            502, 503, 504). Each further consecutive failure doubles it, up to
            `max_reconnect_backoff`, and the count resets only once a connection has stayed
            up for 60 seconds. Must be at least 15 seconds: Massive's
            knowledge base says the server takes "roughly 10 to 30 seconds" to release a
            dropped connection, and an earlier retry can be refused for holding the allowance
            it just lost, which on an individual plan wastes the one connection there is. The
            30-second default waits out that whole window.
        max_reconnect_backoff: Cap, in seconds, of the doubling reconnect backoff. Jitter only
            lengthens a wait, so every wait lies in `[reconnect_backoff,
            max_reconnect_backoff]`. Defaults to 120 seconds.
        max_outage: How long, in seconds, a live table may go without data before it fails.
            A stream fails when its connection has been down this long; a `poll()` table fails when
            its cycles have kept failing, or a late cycle has received nothing, for this long.
            Until then, `m.streams()` says so in `Error`. Call the verb again for a new table.
            `0` fails at the first drop or failed cycle. Defaults to 300 seconds. See
            README.md#live-tables.
        flat_file_symbol_threshold: A count of symbols. With `HistorySource.AUTO`, when both
            flat files and REST can serve a request, this many symbols or more (or `dm.ALL`)
            read flat files. Defaults to 50.
        max_concurrency: Maximum parallel REST requests of one `poll()` or `fetch()` fan-out,
            and of the whole session at once, however many fan-outs overlap; and the most
            flat files the session reads at once. A count of requests. Defaults to 8.
        flush_interval: Seconds between WebSocket micro-batch flushes into the live tables.
            Defaults to 0.05 seconds (50 ms).
        rest_base_url: The REST API base URL. Defaults to `https://api.massive.com`.
        s3_endpoint_url: The flat-file S3 endpoint URL; a user and password in it show as
            `***@` in every log line and message. Defaults to
            `https://files.massive.com`.
        s3_bucket: The flat-file S3 bucket. Defaults to `flatfiles`.
        noncanonical_symbols: What a row of a symbol table (a Deephaven table passed as
            `symbols`) does when it is not Massive's exact ticker, for every verb, whether the
            row is there at the call or added later. `OnError.LOG`, the default, skips the row
            and logs each distinct value once at WARNING (at most 100 values per process);
            `OnError.IGNORE` skips it with nothing logged above DEBUG. Under both, the result
            keeps running on the other rows, a stream's or `poll()` table's `m.streams()` row
            lists the skipped values in `Error` (their count and the first five) until they
            are fixed or deleted, a fixed row is subscribed like any added one, and a table
            whose rows are all bad raises `ValueError` at the call. `OnError.RAISE` raises
            `ValueError` for a bad row present at the call and fails the result when one is
            added later. A `str` or a list passed as `symbols` always raises, whatever this
            says. Defaults to `LOG`.
        unreadable_symbols: What a symbol Massive sends in a spelling the library cannot read
            does. Such a spelling is a gap in the library, not in the caller's code.
            `OnError.LOG`, the default, keeps the row with a null `Symbol` and the text in the
            `RawSymbol` column, and logs each spelling once at ERROR as a bug to report (at
            most 100 per process); `OnError.IGNORE` keeps it the same way with nothing logged
            above DEBUG. Every dataset that reads its symbol from rows has `RawSymbol` under
            both. `OnError.RAISE` fails the request or stream that received the row, naming the
            spelling, and the dataset has no `RawSymbol`. Defaults to `LOG`.
    """

    api_key: str = field(default_factory=_env_api_key, repr=False)
    s3_access_key_id: str | None = field(default_factory=_env_s3_access_key_id)
    s3_secret_access_key: str | None = field(default_factory=_env_s3_secret_access_key, repr=False)
    ws_hosts: Mapping[AssetClass, str] = field(default_factory=dict)
    ack_timeout: float = 5.0
    reconnect_backoff: float = 30.0
    max_reconnect_backoff: float = 120.0
    max_outage: float = 300.0
    flat_file_symbol_threshold: int = 50
    max_concurrency: int = 8
    rest_basic_subscriptions: AbstractSet[Subscription] = frozenset()
    delayed_subscriptions: AbstractSet[Subscription] = frozenset()
    flush_interval: float = 0.05
    rest_base_url: str = "https://api.massive.com"
    s3_endpoint_url: str = "https://files.massive.com"
    s3_bucket: str = "flatfiles"
    noncanonical_symbols: OnError = OnError.LOG
    unreadable_symbols: OnError = OnError.LOG

    def __post_init__(self) -> None:
        """Validate every field; freeze `ws_hosts` and the two subscription sets into copies.

        A `noncanonical_symbols` or `unreadable_symbols` that is not a `dm.OnError`, a
        `ws_hosts` key that is not a `dm.AssetClass`, a host, URL, or bucket that is not a
        `str`, a `rest_basic_subscriptions` or `delayed_subscriptions` that is not a set of
        `dm.Subscription`, or a numeric field of a non-numeric type raises `TypeError`; a
        number that is not positive raises `ValueError`, though `max_outage` may be zero.

        Raises:
            TypeError: If `api_key` is not a `str`.
            AuthenticationError: If no API key was given and none is in the environment.
            ValueError: If `ack_timeout` is not below 60 seconds, `reconnect_backoff` is
                below 15 seconds, `max_reconnect_backoff` is below `reconnect_backoff`,
                `max_outage` is negative or not finite, a timeout, backoff, interval, or count
                is not positive, only one of the two S3 credentials is set, `api_key` or an S3
                credential has leading or trailing whitespace (or an S3 credential is blank),
                `rest_base_url` or `s3_endpoint_url` is not an `http` or `https` URL with a host,
                `s3_bucket` is blank, a `ws_hosts` value is not a bare lower-case hostname, an
                environment variable read for a default has leading or trailing whitespace,
                `delayed_subscriptions` holds `dm.Subscription.CURRENCIES`, or a `ws_hosts`
                pin of a delayed host does not match `delayed_subscriptions`.
        """
        if not isinstance(self.api_key, str):
            raise TypeError(f"api_key must be a str, not {type(self.api_key).__name__}")
        if not self.api_key.strip():
            raise AuthenticationError(
                "no Massive API key: pass MassiveConfig(api_key=...) or set MASSIVE_API_KEY"
            )
        if self.api_key != self.api_key.strip():
            raise ValueError(
                "api_key has leading or trailing whitespace; pass the key without any (the key "
                "is not shown)"
            )
        _check_s3_credential("s3_access_key_id", self.s3_access_key_id)
        _check_s3_credential("s3_secret_access_key", self.s3_secret_access_key)
        if (self.s3_access_key_id is None) != (self.s3_secret_access_key is None):
            raise ValueError(
                "flat files need both s3_access_key_id and s3_secret_access_key "
                "(MASSIVE_S3_ACCESS_KEY_ID and MASSIVE_S3_SECRET_ACCESS_KEY)"
            )
        for name in ("ack_timeout", "reconnect_backoff", "max_reconnect_backoff", "flush_interval"):
            require_positive_number(name, getattr(self, name))
        for name in ("flat_file_symbol_threshold", "max_concurrency"):
            require_positive_int(name, getattr(self, name))
        if self.reconnect_backoff < MIN_RECONNECT_BACKOFF:
            raise ValueError(
                f"reconnect_backoff must be at least {MIN_RECONNECT_BACKOFF:g} s, not "
                f"{self.reconnect_backoff:g}: Massive takes roughly 10 to 30 s to release a "
                "dropped connection, and a sooner retry can be refused for holding it"
            )
        if self.ack_timeout >= NO_ACK_RESEND:
            raise ValueError(
                f"ack_timeout must be below {NO_ACK_RESEND:g} s, not {self.ack_timeout:g}: "
                f"unacknowledged subscriptions are re-sent every {NO_ACK_RESEND:g} s, and each "
                "re-send awaits its answer for ack_timeout"
            )
        if self.max_reconnect_backoff < self.reconnect_backoff:
            raise ValueError("max_reconnect_backoff must not be below reconnect_backoff")
        object.__setattr__(self, "max_outage", _check_max_outage(self.max_outage))
        _require_enum(self.noncanonical_symbols, OnError, "noncanonical_symbols")
        _require_enum(self.unreadable_symbols, OnError, "unreadable_symbols")
        _check_url("rest_base_url", self.rest_base_url)
        _check_url("s3_endpoint_url", self.s3_endpoint_url)
        _check_bucket(self.s3_bucket)
        object.__setattr__(self, "ws_hosts", _validate_ws_hosts(self.ws_hosts))
        basic = _validate_subscriptions(self.rest_basic_subscriptions, "rest_basic_subscriptions")
        object.__setattr__(self, "rest_basic_subscriptions", basic)
        delayed = _validate_subscriptions(self.delayed_subscriptions, "delayed_subscriptions")
        _check_delayed_subscriptions(delayed, self.ws_hosts)
        object.__setattr__(self, "delayed_subscriptions", delayed)

    @classmethod
    def from_file(cls, path: str | os.PathLike[str] | None = None) -> MassiveConfig:
        """Build a configuration from a YAML settings file, for settings shared between scripts.

        The file is a mapping of setting names, the attribute names of this class, to values;
        a setting it leaves out takes its default. Precedence is code, then the file, then the
        defaults: override a setting of the file in code with `dataclasses.replace`. The
        credentials are never read from the file; they come from code or from their
        environment variables, as for `MassiveConfig()`. Every scalar is read as text and
        converted by the setting's own rule, so a ticker such as `ON` or `NO` stays text, and
        an enum word is taken only as spelled exactly (`raise`, not `Raise`). The README's
        "Settings file" section shows a full file (README.md#settings-file).

        Example:
            ::

                import dataclasses

                import deephaven_massive as dm

                # settings.yaml holds, for example:
                #   delayed_subscriptions: [options]
                #   max_outage: 600
                cfg = dm.MassiveConfig.from_file("~/massive/settings.yaml")
                cfg = dataclasses.replace(cfg, max_concurrency=4)  # code wins over the file
                m = dm.MassiveSession(cfg)

        Args:
            path: The file, as a `str` or path; a leading `~` is expanded to the home folder.
                Defaults to the path in the `DEEPHAVEN_MASSIVE_CONFIG_FILE` environment
                variable, the way a console or container names its file without code.

        Returns:
            The validated configuration.

        Raises:
            ValueError: If `path` is not given and `DEEPHAVEN_MASSIVE_CONFIG_FILE` is unset or
                blank (or has surrounding whitespace); if the file is not UTF-8 or not valid
                YAML; if it holds more than one document, a top level that is not a mapping
                (an empty file included), a duplicate key, a tag (`!!python/...` or any
                other), an anchor or alias, a credential (the message names the environment
                variable to use and never shows the value), or an unknown setting (the
                message suggests the closest name, and shows at most the key's first 40
                characters); if a value is empty or cannot be converted; or if the constructor
                refuses a value. Each message names the file, and the setting and its line and
                column where known, except one about an environment variable (a credential
                with surrounding whitespace, or one S3 credential without the other), which
                names the variable instead.
            FileNotFoundError: If the file does not exist; the message names it, and the
                environment variable when the path came from there.
            OSError: If the file exists but cannot be read.
            AuthenticationError: If no API key is in the environment.

        Side effects:
            Reads the file once, and logs one INFO line naming it and the settings it set.
        """
        try:
            return _read_settings_file(path)
        except (ValueError, OSError) as error:
            logger.debug("MassiveConfig.from_file(%r) refused: %s", path, error)
            raise

    @override
    def __repr__(self) -> str:
        """Show every setting but the secrets, with no user or password in a URL.

        The API key and the S3 secret are left out, as the fields declare, and a URL
        (`rest_base_url`, `s3_endpoint_url`) shows `***@` in place of a user and password in
        it, so the text is safe to log (`shown_setting`).

        Returns:
            `"MassiveConfig(s3_access_key_id=None, ws_hosts={}, ...)"`, field by field.
        """
        shown = ", ".join(
            f"{f.name}={shown_setting(f.name, getattr(self, f.name))}"
            for f in fields(self)
            if f.repr
        )
        return f"MassiveConfig({shown})"


_URL_SETTINGS: Final = frozenset({"rest_base_url", "s3_endpoint_url"})
"""The `MassiveConfig` fields that hold a URL, which a user may write with credentials."""


def shown_setting(name: str, value: object) -> str:
    """Return the text a log line or `repr` shows for one `MassiveConfig` setting.

    Args:
        name: The field's name.
        value: Its value.

    Returns:
        The value's `repr`; for a URL setting, with `***@` in place of a user and password
        (`without_userinfo`).
    """
    if name in _URL_SETTINGS and isinstance(value, str):
        return repr(without_userinfo(value))
    return repr(value)


def require_positive_int(name: str, value: object, *, maximum: int | None = None) -> int:
    """Return an `int` argument after checking that it is positive (and at most `maximum`).

    The one check behind every count-like parameter: `limit`, `multiplier`, `ring` capacity,
    and the integer configuration fields.

    Args:
        name: The parameter's name, for messages.
        value: The argument as passed.
        maximum: The largest accepted value, or None for no upper bound.

    Returns:
        `value`, narrowed to `int`.

    Raises:
        TypeError: If `value` is not an `int` (a `bool` is not accepted).
        ValueError: If `value` is below 1 or above `maximum`.
    """
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an int, not {type(value).__name__}")
    if maximum is not None and not 1 <= value <= maximum:
        raise ValueError(f"{name} must be between 1 and {maximum}, got {value}")
    if value < 1:
        raise ValueError(f"{name} must be positive, got {value}")
    return value


def require_positive_number(name: str, value: object) -> float:
    """Return a numeric argument as a `float` after checking that it is finite and positive.

    The one check behind every duration-like parameter: the configuration timeouts and
    intervals and the numeric form of `poll()`'s interval.

    Args:
        name: The parameter's name, for messages.
        value: The argument as passed.

    Returns:
        `value` as a `float`.

    Raises:
        TypeError: If `value` is not an `int` or `float` (a `bool` is not accepted).
        ValueError: If `value` is not finite, not greater than zero, or an `int` too large
            for a `float`.
    """
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise TypeError(f"{name} must be a number, not {type(value).__name__}")
    try:
        number = float(value)
    except OverflowError:  # an int beyond the float range
        raise ValueError(
            f"{name} must be positive and finite, got an int too large for a float"
        ) from None
    if not (math.isfinite(number) and number > 0):
        raise ValueError(f"{name} must be positive, got {value!r}")
    return number


_CURRENCY_CLASSES: Final = frozenset(
    c.value for c in AssetClass if subscription_of(c) is Subscription.CURRENCIES
)
"""The asset classes that share one subscription, `Subscription.CURRENCIES` (`forex`, `crypto`)."""


def _validate_subscriptions(value: object, name: str) -> frozenset[Subscription]:
    """Validate a set of subscriptions (`rest_basic_subscriptions`, `delayed_subscriptions`).

    Args:
        value: The value passed for the field.
        name: The field's name, for messages.

    Returns:
        The subscriptions, as a `frozenset`.

    Raises:
        TypeError: If `value` is not a set (a `set`, a `frozenset`, or another
            `collections.abc.Set`), or holds anything but `dm.Subscription` members. Forex
            or crypto (as text or as a `dm.AssetClass`) is refused with a message naming
            `dm.Subscription.CURRENCIES`, the one subscription that covers both; any other
            text names the matching member, as `_require_enum` does.
    """
    if not isinstance(value, AbstractSet):
        raise TypeError(
            f"{name} must be a set of dm.Subscription, such as "
            f"{{dm.Subscription.STOCKS}}, not {type(value).__name__}"
        )
    for item in value:
        if (
            isinstance(item, str)
            and not isinstance(item, Subscription)
            and item.casefold() in _CURRENCY_CLASSES
        ):
            raise TypeError(
                f"{name} must hold dm.Subscription members; forex and crypto "
                "are one subscription, dm.Subscription.CURRENCIES"
            )
        _require_enum(item, Subscription, f"each {name} member")
    return frozenset(value)


DELAYED_PREFIX: Final = "delayed."
"""The start of a delayed-feed hostname, such as `delayed.massive.com`."""


def _check_delayed_subscriptions(
    delayed: frozenset[Subscription], ws_hosts: Mapping[AssetClass, str]
) -> None:
    """Refuse a `delayed_subscriptions` Massive cannot serve, or one a `ws_hosts` pin contradicts.

    A host whose name starts with `delayed.` is a delayed feed: pinning one is asking for
    delayed data, which only a listed subscription may do, and a listed subscription may pin
    nothing else.

    Args:
        delayed: The validated `delayed_subscriptions`.
        ws_hosts: The validated `ws_hosts`.

    Raises:
        ValueError: If `delayed` holds `dm.Subscription.CURRENCIES` (Massive has no delayed
            forex or crypto feed), or `ws_hosts` pins a `delayed.` host for forex or crypto,
            or for a class whose subscription is not listed, or pins any other host for one
            that is. Each message names both settings and what to change.
    """
    if Subscription.CURRENCIES in delayed:
        raise ValueError(
            "delayed_subscriptions cannot hold dm.Subscription.CURRENCIES: Massive has no "
            "delayed forex or crypto feed, so their streams are always real-time"
        )
    for asset_class, host in ws_hosts.items():
        subscription = subscription_of(asset_class)
        listed = subscription in delayed
        pins_delayed = host.startswith(DELAYED_PREFIX)
        if pins_delayed and subscription is Subscription.CURRENCIES:
            raise ValueError(
                f"ws_hosts pins {asset_class} to {host}, but Massive has no delayed forex or "
                "crypto feed; remove the pin"
            )
        if pins_delayed and not listed:
            raise ValueError(
                f"ws_hosts pins {asset_class} to the delayed feed {host}, but "
                f"delayed_subscriptions does not list dm.Subscription.{subscription.name}; "
                f"to use the delayed feed write {delayed_fix(subscription)}"
            )
        if listed and not pins_delayed:
            raise ValueError(
                f"delayed_subscriptions lists dm.Subscription.{subscription.name}, "
                f"whose streams use Massive's delayed feed {DELAYED_HOST}, but ws_hosts pins "
                f"{asset_class} to {host}; remove one of the two"
            )


def delayed_fix(subscription: Subscription) -> str:
    """Return the configuration that asks for a subscription's delayed data, for messages.

    The one place the setting's spelling appears in the library's messages.

    Args:
        subscription: The subscription.

    Returns:
        For example `dm.MassiveConfig(delayed_subscriptions={dm.Subscription.OPTIONS})`.
    """
    return f"dm.MassiveConfig(delayed_subscriptions={{dm.Subscription.{subscription.name}}})"


def _check_max_outage(value: object) -> float:
    """Return `max_outage` as a `float` after checking that it is a finite, non-negative number.

    Zero is valid, unlike the other durations: it fails live tables at the first disconnect.

    Args:
        value: The value passed as `MassiveConfig.max_outage`.

    Returns:
        The seconds, as a `float`.

    Raises:
        TypeError: If `value` is not an `int` or `float` (a `bool` is not accepted).
        ValueError: If `value` is negative, not finite, or an `int` too large for a `float`.
    """
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise TypeError(f"max_outage must be a number of seconds, not {type(value).__name__}")
    try:
        seconds = float(value)
    except OverflowError:  # an int beyond the float range
        raise ValueError("max_outage must be finite, got an int too large for a float") from None
    if not (math.isfinite(seconds) and seconds >= 0):
        raise ValueError(
            f"max_outage must be zero or more seconds, got {value!r}; 0 fails live tables at "
            "the first disconnect"
        )
    return seconds


def _check_s3_credential(name: str, value: str | None) -> None:
    """Refuse an S3 credential that is blank or has surrounding whitespace.

    Args:
        name: The field's name, for the message.
        value: The credential, or None when absent.

    Raises:
        ValueError: If `value` is blank text or has leading or trailing whitespace. The
            message names the field but never the value, a secret.
    """
    if value is not None and (not value.strip() or value != value.strip()):
        raise ValueError(
            f"{name} is blank or has leading or trailing whitespace; pass the credential "
            "exactly, or None for no flat files (the value is not shown)"
        )


_HTTP_URL: Final = re.compile(
    r"https?://(?:[^/?#\s]*@)?(?:\[[0-9A-Fa-f:.]+\]|[^/?#@:\s\[\]]+)(?::[0-9]*)?(?:[/?#]\S*)?"
)
"""An `http` or `https` URL with a host and no whitespace: the scheme, an optional user and
password, a host name or bracketed IPv6 address, an optional port, and an optional path."""


def _check_url(name: str, value: object) -> None:
    """Refuse a base URL that is not an `http` or `https` URL with a host.

    Every request the library makes is built on the URL, so a value such as `""`, `~`, or
    `api.massive.com` (no scheme) is refused here rather than failing on the first request.

    Args:
        name: The field's name (`rest_base_url` or `s3_endpoint_url`), for the message.
        value: The field's value.

    Raises:
        TypeError: If `value` is not a `str`.
        ValueError: If `value` holds whitespace, or its scheme is not `http` or `https`, or it
            names no host. The message shows the value with `***@` in place of a user and
            password (`shown_setting`).
    """
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a str, not {type(value).__name__}")
    if not _HTTP_URL.fullmatch(value):
        raise ValueError(
            f"{name} must be an http or https URL with a host, such as "
            f"'https://api.massive.com', not {shown_setting(name, value)}"
        )


def _check_bucket(value: object) -> None:
    """Refuse an `s3_bucket` that is not text or is blank.

    Args:
        value: The field's value.

    Raises:
        TypeError: If `value` is not a `str`.
        ValueError: If `value` is blank.
    """
    if not isinstance(value, str):
        raise TypeError(f"s3_bucket must be a str, not {type(value).__name__}")
    if not value.strip():
        raise ValueError(f"s3_bucket must name a bucket, such as 'flatfiles', not {value!r}")


def _validate_ws_hosts(ws_hosts: object) -> Mapping[AssetClass, str]:
    """Validate the `ws_hosts` mapping and return a read-only, normalized copy.

    Args:
        ws_hosts: The value passed as `MassiveConfig.ws_hosts`.

    Returns:
        A read-only mapping of the hostnames as passed.

    Raises:
        TypeError: If `ws_hosts` is not a mapping, a key is not a `dm.AssetClass`, or a value
            is not a `str`.
        ValueError: If a value is not a bare lower-case hostname (a scheme, port, path,
            upper-case letter, or surrounding whitespace is present); nothing is cleaned, and
            a value that would be a hostname once trimmed and lower-cased names that form.
    """
    if not isinstance(ws_hosts, Mapping):
        raise TypeError("ws_hosts must be a mapping of dm.AssetClass to hostname")
    hosts: dict[AssetClass, str] = {}
    for key, value in ws_hosts.items():
        asset_class = _require_enum(key, AssetClass, "ws_hosts key")
        if not isinstance(value, str):
            raise TypeError(f"ws_hosts[{asset_class}] must be a hostname str")
        if value != value.strip().lower() and _HOSTNAME.match(value.strip().lower()):
            raise ValueError(
                f"ws_hosts[{asset_class}]={value!r} must be written exactly as a lower-case "
                f"hostname; write {value.strip().lower()!r}"
            )
        if not _HOSTNAME.match(value):
            raise ValueError(
                f"ws_hosts[{asset_class}] must be a bare hostname such as "
                f"'delayed.massive.com', got {without_userinfo(value)!r}"
            )
        hosts[asset_class] = value
    return HostMap(hosts)


class HostMap(Mapping[AssetClass, str]):
    """The read-only, hashable `ws_hosts` mapping of a `MassiveConfig`.

    `MassiveConfig` is a frozen dataclass compared and hashed by value, so its fields must be
    hashable; a dict or `MappingProxyType` is not. Equal mappings hash equally.
    """

    __slots__ = ("_hosts",)

    def __init__(self, hosts: Mapping[AssetClass, str]) -> None:
        """Copy the validated hosts.

        Args:
            hosts: Hostname per asset class.
        """
        self._hosts: Mapping[AssetClass, str] = dict(hosts)

    @override
    def __getitem__(self, key: AssetClass) -> str:
        """Return the host pinned for an asset class.

        Args:
            key: The asset class.

        Returns:
            The hostname.
        """
        return self._hosts[key]

    @override
    def __iter__(self) -> Iterator[AssetClass]:
        """Iterate over the pinned asset classes.

        Returns:
            An iterator of asset classes.
        """
        return iter(self._hosts)

    @override
    def __len__(self) -> int:
        """Count the pinned asset classes.

        Returns:
            How many hosts are pinned.
        """
        return len(self._hosts)

    @override
    def __hash__(self) -> int:
        """Hash by content, so equal mappings hash equally.

        Returns:
            The hash of the frozen set of items.
        """
        return hash(frozenset(self._hosts.items()))

    @override
    def __repr__(self) -> str:
        """Show the mapping as a dict literal.

        Returns:
            For example `{<AssetClass.STOCKS: 'stocks'>: 'delayed.massive.com'}`.
        """
        return repr(dict(self._hosts))


# --- The settings file -------------------------------------------------------------------------


class _Position(Protocol):
    """Where in a file something is: PyYAML's mark, zero-based, in either of its two forms."""

    line: int
    column: int


class _SettingsFileError(Exception):
    """A problem at one place in a settings file, before the file's name and the key are added.

    `_read_settings_file` turns it into the `ValueError` the caller sees.
    """

    def __init__(self, problem: str, position: _Position | None) -> None:
        """Record the problem and where it is.

        Args:
            problem: What is wrong and what to write instead.
            position: Where it is, or None when PyYAML gives no place.
        """
        super().__init__(problem)
        self.problem = problem
        self.position = position


def _place(file: Path, position: _Position | None) -> str:
    """Return the start of a settings-file message: the file, and the line and column if known.

    Args:
        file: The settings file.
        position: PyYAML's zero-based mark, or None.

    Returns:
        For example `"settings file /home/me/settings.yaml, line 3, column 14"`.
    """
    if position is None:
        return f"settings file {file}"
    return f"settings file {file}, line {position.line + 1}, column {position.column + 1}"


class _SettingsLoader(yaml.BaseLoader):
    """PyYAML's pure-Python base loader, refusing tags, anchors, and aliases.

    The base loader resolves no implicit types, so every scalar is text: `ON`, `NO`, `Y`,
    `off`, and `0700` stay as written instead of becoming booleans or numbers. It is pure
    Python, so it neither depends on libyaml nor builds any object a tag names. The settings
    file is read only as a node tree (`compose_document`), never constructed, so the nodes
    keep their positions for messages.
    """

    @override
    def compose_node(self, parent: yaml.Node | None, index: int) -> yaml.Node | None:
        """Compose one node after refusing an alias, an anchor, or an explicit tag on it.

        Args:
            parent: The enclosing node, passed through.
            index: The node's place in its parent, passed through.

        Returns:
            The node.

        Raises:
            _SettingsFileError: If the node is an alias (`*name`), defines an anchor
                (`&name`), or carries an explicit tag (`!name`, `!!python/object`, ...).
        """
        peek: Callable[[], yaml.Event] = self.peek_event  # PyYAML's stubs leave it untyped
        event = peek()
        if isinstance(event, yaml.AliasEvent):
            raise _SettingsFileError(
                "an alias (*name) is not allowed; write the value out", event.start_mark
            )
        if isinstance(event, yaml.NodeEvent) and event.anchor is not None:
            raise _SettingsFileError(
                f"an anchor (&{event.anchor}) is not allowed; write each value out",
                event.start_mark,
            )
        if isinstance(event, yaml.ScalarEvent | yaml.CollectionStartEvent) and event.tag:
            raise _SettingsFileError(
                f"a tag ({event.tag}) is not allowed; write the plain value", event.start_mark
            )
        return super().compose_node(parent, index)


def _compose(text: str) -> yaml.MappingNode:
    """Parse a settings file's text into its one top-level mapping node.

    Args:
        text: The file's content.

    Returns:
        The top-level mapping.

    Raises:
        _SettingsFileError: If the text holds no document, more than one, or a top level that
            is not a mapping, or `_SettingsLoader` refuses a node.
        yaml.YAMLError: If the text is not valid YAML.
    """
    loader = _SettingsLoader(text)
    # PyYAML's stubs leave the two event readers untyped.
    get_event: Callable[[], yaml.Event] = loader.get_event
    peek_event: Callable[[], yaml.Event] = loader.peek_event
    try:
        get_event()  # the stream's start
        node = None if loader.check_event(yaml.StreamEndEvent) else loader.compose_document()
        if not loader.check_event(yaml.StreamEndEvent):
            raise _SettingsFileError(
                "a second document starts here; a settings file holds one",
                peek_event().start_mark,
            )
    finally:
        loader.dispose()
    if not isinstance(node, yaml.MappingNode):
        raise _SettingsFileError(
            "the top level must be a mapping of setting names to values, such as 'max_outage: 600'",
            None if node is None else node.start_mark,
        )
    return node


def _scalar(node: yaml.Node, expected: str) -> str:
    """Return a scalar node's text, or refuse a list or mapping where text belongs.

    Args:
        node: The value's node.
        expected: What the setting takes, for the message, such as `"a number of seconds"`.

    Returns:
        The text exactly as written; YAML has already removed the quotes of a quoted value.

    Raises:
        _SettingsFileError: If the node is a list or a mapping.
    """
    if not isinstance(node, yaml.ScalarNode):
        raise _SettingsFileError(f"must be {expected}, not a list or mapping", node.start_mark)
    text: str = node.value
    return text


_Word = TypeVar("_Word", bound=StrEnum)


def _word(node: yaml.Node, enum_type: type[_Word], what: str) -> _Word:
    """Return the enum member whose value is exactly a scalar's text.

    The text must match a member's value exactly: `raise`, not `Raise` or `" raise"`, as the
    library takes every word as written and never repairs one.

    Args:
        node: The value's node.
        enum_type: The enum.
        what: What a member is, for the message, such as `"a subscription"`.

    Returns:
        The member.

    Raises:
        _SettingsFileError: If the node is not a scalar, or its text is no member's value; the
            message lists the valid spellings.
    """
    spellings = ", ".join(member.value for member in enum_type)
    text = _scalar(node, f"{what} ({spellings})")
    for member in enum_type:
        if member.value == text:
            return member
    raise _SettingsFileError(
        f"{text!r} is not {what}; write one of {spellings}, exactly as shown", node.start_mark
    )


def _on_error(node: yaml.Node) -> OnError:
    """Convert a symbol setting's word: `raise`, `log`, or `ignore` (`_word`).

    Args:
        node: The value's node.

    Returns:
        The `OnError` member.
    """
    return _word(node, OnError, "a symbol setting")


def _subscriptions(node: yaml.Node) -> frozenset[Subscription]:
    """Convert a list of subscription words, such as `[stocks, options]` (`_word`).

    Args:
        node: The value's node.

    Returns:
        The subscriptions.

    Raises:
        _SettingsFileError: If the node is not a list, or a member is not a subscription's word or
            is listed twice.
    """
    if not isinstance(node, yaml.SequenceNode):
        raise _SettingsFileError(
            "must be a list of subscriptions, such as [stocks, options], or [] for none",
            node.start_mark,
        )
    found: set[Subscription] = set()
    for item in node.value:
        subscription = _word(item, Subscription, "a subscription")
        if subscription in found:
            raise _SettingsFileError(f"lists {subscription.value!r} twice", item.start_mark)
        found.add(subscription)
    return frozenset(found)


def _hosts(node: yaml.Node) -> dict[AssetClass, str]:
    """Convert `ws_hosts`: a mapping of asset-class word to hostname.

    Args:
        node: The value's node.

    Returns:
        The hostname per asset class, as written; the constructor checks each one.

    Raises:
        _SettingsFileError: If the node is not a mapping, a key is not an asset class's word or
            appears twice, or a hostname is not text.
    """
    if not isinstance(node, yaml.MappingNode):
        raise _SettingsFileError(
            "must be a mapping of asset class to hostname, such as {stocks: delayed.massive.com}",
            node.start_mark,
        )
    hosts: dict[AssetClass, str] = {}
    for key, value in node.value:
        asset_class = _word(key, AssetClass, "an asset class")
        if asset_class in hosts:
            raise _SettingsFileError(f"pins {asset_class.value!r} twice", key.start_mark)
        hosts[asset_class] = _scalar(value, "a hostname")
    return hosts


_DECIMAL: Final = re.compile(r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?")
"""A decimal number as a settings file writes one: `300`, `0.05`, `1e3`; no leading zeros."""

_WHOLE: Final = re.compile(r"-?(?:0|[1-9][0-9]*)")
"""A whole number as a settings file writes one: `8`, `50`; no leading zeros."""


def _seconds(node: yaml.Node) -> float:
    """Convert a duration: a finite decimal number of seconds, such as `300` or `0.05`.

    Args:
        node: The value's node.

    Returns:
        The number; the constructor checks its range.

    Raises:
        _SettingsFileError: If the text is not a decimal number, or is too large to be finite.
    """
    text = _scalar(node, "a number of seconds")
    number = float(text) if _DECIMAL.fullmatch(text) else math.nan
    if not math.isfinite(number):
        raise _SettingsFileError(
            f"{text!r} is not a finite number of seconds; write one such as 300 or 0.5",
            node.start_mark,
        )
    return number


def _count(node: yaml.Node) -> int:
    """Convert a count: a whole number, such as `8`.

    Args:
        node: The value's node.

    Returns:
        The number; the constructor checks its range.

    Raises:
        _SettingsFileError: If the text is not a whole decimal number, or has more digits than
            Python converts (`sys.get_int_max_str_digits`); that message leaves the text out.
    """
    text = _scalar(node, "a whole number")
    if not _WHOLE.fullmatch(text):
        raise _SettingsFileError(
            f"{text!r} is not a whole number; write one such as 8", node.start_mark
        )
    try:
        return int(text)
    except ValueError:  # more digits than Python's integer conversion limit
        raise _SettingsFileError(
            f"is a number of {len(text)} characters, too long to read; write one such as 8",
            node.start_mark,
        ) from None


_NULL_WORDS: Final = frozenset({"", "~", "null", "Null", "NULL"})
"""The plain (unquoted) scalars YAML reads as no value at all."""


def _text(node: yaml.Node) -> str:
    """Convert a text setting (a URL or the bucket), taken exactly as written.

    A plain value YAML means as empty (`rest_base_url:` with nothing after it, `~`, or
    `null`) is refused rather than taken as the text `""` or `"~"`: a user who writes one
    means the default, which the file gives by leaving the setting out.

    Args:
        node: The value's node.

    Returns:
        The text.

    Raises:
        _SettingsFileError: If the node is a list or mapping, is empty quoted text (`''`), or
            is a plain empty value (`_NULL_WORDS`).
    """
    text = _scalar(node, "text")
    plain = isinstance(node, yaml.ScalarNode) and node.style is None
    if not text or (plain and text in _NULL_WORDS):
        raise _SettingsFileError(
            "is empty; write the value, or leave the setting out for its default",
            node.start_mark,
        )
    return text


_CONVERTERS: Final[Mapping[str, Callable[[yaml.Node], object]]] = {
    "ws_hosts": _hosts,
    "ack_timeout": _seconds,
    "reconnect_backoff": _seconds,
    "max_reconnect_backoff": _seconds,
    "max_outage": _seconds,
    "flat_file_symbol_threshold": _count,
    "max_concurrency": _count,
    "rest_basic_subscriptions": _subscriptions,
    "delayed_subscriptions": _subscriptions,
    "flush_interval": _seconds,
    "rest_base_url": _text,
    "s3_endpoint_url": _text,
    "s3_bucket": _text,
    "noncanonical_symbols": _on_error,
    "unreadable_symbols": _on_error,
}
"""The converter of each setting a file may hold: every field of `MassiveConfig` but the
credentials."""

_CREDENTIAL_VARIABLES: Final[Mapping[str, str]] = {
    "api_key": "MASSIVE_API_KEY",
    "s3_access_key_id": "MASSIVE_S3_ACCESS_KEY_ID",
    "s3_secret_access_key": "MASSIVE_S3_SECRET_ACCESS_KEY",
}
"""The credentials a settings file may not hold, and the environment variable of each."""

_SETTING_NAME: Final = re.compile(r"(?:each )?([a-z0-9_]+)")
"""The setting a constructor message starts with, such as `max_outage` or `each ... member`."""

_SHOWN_KEY_LENGTH: Final = 40
"""The most characters of an unknown key a message shows, so a pasted secret is cut short."""


def _settings_path(path: str | os.PathLike[str] | None) -> tuple[Path, str]:
    """Return the settings file's path, with `~` expanded, and where the path came from.

    Args:
        path: The path passed to `from_file`, or None to read `CONFIG_FILE_VARIABLE`.

    Returns:
        The path, and `""` or, for a path from the environment, a note naming the variable.

    Raises:
        ValueError: If `path` is None and the variable is unset, blank, or padded.
    """
    if path is not None:
        return Path(path).expanduser(), ""
    text = environment_text(CONFIG_FILE_VARIABLE)
    if text is None:
        raise ValueError(
            f"no settings file: pass its path to MassiveConfig.from_file, or set "
            f"{CONFIG_FILE_VARIABLE} to it"
        )
    return Path(text).expanduser(), f" (from {CONFIG_FILE_VARIABLE})"


def _settings_values(
    file: Path, root: yaml.MappingNode
) -> tuple[dict[str, Any], dict[str, _Position]]:
    """Convert a settings file's top-level mapping into keyword arguments of `MassiveConfig`.

    Args:
        file: The settings file, for messages.
        root: Its top-level mapping.

    Returns:
        The converted value of each setting, and where each setting's key is in the file, for
        the constructor's messages.

    Raises:
        ValueError: If a key appears twice, is a credential, or is unknown, or a converter
            refuses a value. The message names the file, the line and column, and the key.
        _SettingsFileError: If a key is not text.
    """
    values: dict[str, Any] = {}
    positions: dict[str, _Position] = {}
    for key, value in root.value:
        name = _scalar(key, "a setting name")
        place = _place(file, key.start_mark)
        if name in positions:
            raise ValueError(
                f"{place}: {name} is set twice; set it once (it is first set on line "
                f"{positions[name].line + 1})"
            )
        if name in _CREDENTIAL_VARIABLES:
            raise ValueError(
                f"{place}: {name} is a credential, which a settings file may not hold; remove "
                f"it and set {_CREDENTIAL_VARIABLES[name]} in the environment, or pass it in "
                "code (the value is not shown)"
            )
        convert = _CONVERTERS.get(name)
        if convert is None:
            known = [*_CONVERTERS, *_CREDENTIAL_VARIABLES]
            close = difflib.get_close_matches(name, known, n=1)
            hint = (
                f"did you mean {close[0]!r}?"
                if close
                else f"the settings are {', '.join(_CONVERTERS)}"
            )
            shown = name if len(name) <= _SHOWN_KEY_LENGTH else name[:_SHOWN_KEY_LENGTH] + "…"
            raise ValueError(f"{place}: unknown setting {shown!r}; {hint}")
        try:
            values[name] = convert(value)
        except _SettingsFileError as problem:
            where = _place(file, problem.position)
            raise ValueError(f"{where}: {name} {problem.problem}") from None
        positions[name] = key.start_mark
    return values, positions


def _read_settings_file(path: str | os.PathLike[str] | None) -> MassiveConfig:
    """Read, convert, and validate a settings file; `MassiveConfig.from_file` documents it.

    Args:
        path: The file, or None to read `CONFIG_FILE_VARIABLE`.

    Returns:
        The validated configuration.

    Raises:
        ValueError: As `from_file` says.
        FileNotFoundError: If the file does not exist.
    """
    file, source = _settings_path(path)
    try:
        text = file.read_text(encoding="utf-8")
    except FileNotFoundError:
        raise FileNotFoundError(f"settings file {file} does not exist{source}") from None
    except UnicodeDecodeError as error:
        raise ValueError(
            f"settings file {file} is not UTF-8 text: {error.reason} at byte {error.start}"
        ) from None
    try:
        values, positions = _settings_values(file, _compose(text))
    except _SettingsFileError as problem:
        raise ValueError(f"{_place(file, problem.position)}: {problem.problem}") from None
    except yaml.MarkedYAMLError as error:
        raise ValueError(f"{_place(file, error.problem_mark)}: {error.problem}") from None
    except yaml.YAMLError as error:
        raise ValueError(f"settings file {file}: {error}") from None
    try:
        config = MassiveConfig(**values)
    except ValueError as error:
        # A message about a setting a file may hold gets the file and the line; one about the
        # environment (a padded variable, one S3 credential without the other) already names
        # its variable, and the file has no part in it.
        match = _SETTING_NAME.match(str(error))
        if match is None or match.group(1) not in _CONVERTERS:
            raise
        where = _place(file, positions.get(match.group(1)))
        raise ValueError(f"{where}: {error}") from None
    logger.info("settings file %s%s set %s", file, source, ", ".join(values) or "no settings")
    return config
