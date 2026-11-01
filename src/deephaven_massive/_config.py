"""`MassiveConfig`, the session's immutable configuration.

`MassiveConfig` is the only way to configure the library: `MassiveSession(MassiveConfig(...))`.
Every default lives in exactly one place, the field defaults of the dataclass below; nothing else in
the package repeats them. `MassiveConfig()` with no arguments reads the API key and the S3
credentials from the environment and validates everything at construction, so a bad configuration
fails before any session exists.

What is deliberately *not* configurable: the options contract cap (1,000 per connection), the
evidence endpoints of the host decision, the plan tier (Massive offers no entitlement endpoint,
so the library learns the tier from REST evidence instead), the REST rate of a Basic
subscription (5 requests per minute, a wire constant; `rest_basic_subscriptions` only names the
subscriptions on Basic), and the JSON codec. The wire
package owns the codec: it uses `orjson` when the optional extra `deephaven-massive[orjson]` is
installed and the standard library otherwise, and logs the choice once at INFO.
"""

from __future__ import annotations

import math
import os
import re
from collections.abc import Mapping
from collections.abc import Set as AbstractSet
from dataclasses import dataclass, field, fields
from typing import TYPE_CHECKING, Final

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


def _env_on_error(name: str) -> OnError:
    """Read a symbol setting's environment variable: `raise`, `log`, or `ignore`, in any case.

    Args:
        name: The variable, such as `"DEEPHAVEN_MASSIVE_UNREADABLE_SYMBOLS"`.

    Returns:
        The setting; `OnError.LOG` when the variable is unset or blank.

    Raises:
        ValueError: If the variable holds any other text; the message names the variable and
            the three values.
    """
    text = environment_text(name)
    if text is None:
        return OnError.LOG
    try:
        return OnError(text.lower())
    except ValueError:
        raise ValueError(f"{name} must be 'raise', 'log', or 'ignore', not {text!r}") from None


def _env_noncanonical_symbols() -> OnError:
    """Read `DEEPHAVEN_MASSIVE_NONCANONICAL_SYMBOLS` (`_env_on_error`).

    Returns:
        The setting; `OnError.LOG` when unset.
    """
    return _env_on_error("DEEPHAVEN_MASSIVE_NONCANONICAL_SYMBOLS")


def _env_unreadable_symbols() -> OnError:
    """Read `DEEPHAVEN_MASSIVE_UNREADABLE_SYMBOLS` (`_env_on_error`).

    Returns:
        The setting; `OnError.LOG` when unset.
    """
    return _env_on_error("DEEPHAVEN_MASSIVE_UNREADABLE_SYMBOLS")


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
            says. Defaults to `DEEPHAVEN_MASSIVE_NONCANONICAL_SYMBOLS` (`raise`, `log`, or
            `ignore`, any case), then `LOG`.
        unreadable_symbols: What a symbol Massive sends in a spelling the library cannot read
            does. Such a spelling is a gap in the library, not in the caller's code.
            `OnError.LOG`, the default, keeps the row with a null `Symbol` and the text in the
            `RawSymbol` column, and logs each spelling once at ERROR as a bug to report (at
            most 100 per process); `OnError.IGNORE` keeps it the same way with nothing logged
            above DEBUG. Every dataset that reads its symbol from rows has `RawSymbol` under
            both. `OnError.RAISE` fails the request or stream that received the row, naming the
            spelling, and the dataset has no `RawSymbol`. Defaults to
            `DEEPHAVEN_MASSIVE_UNREADABLE_SYMBOLS` (`raise`, `log`, or `ignore`, any case),
            then `LOG`.
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
    noncanonical_symbols: OnError = field(default_factory=_env_noncanonical_symbols)
    unreadable_symbols: OnError = field(default_factory=_env_unreadable_symbols)

    def __post_init__(self) -> None:
        """Validate every field; freeze `ws_hosts` and the two subscription sets into copies.

        A `noncanonical_symbols` or `unreadable_symbols` that is not a `dm.OnError`, a
        `ws_hosts` key that is not a `dm.AssetClass`, a host that is not a `str`, a
        `rest_basic_subscriptions` or `delayed_subscriptions` that is not a set of
        `dm.Subscription`, or a numeric field of a non-numeric type raises `TypeError`; a
        number that is not positive raises `ValueError`, though `max_outage` may be zero.

        Raises:
            TypeError: If `api_key` is not a `str`.
            AuthenticationError: If no API key was given and none is in the environment.
            ValueError: If `ack_timeout` is not below 60 seconds, `reconnect_backoff` is
                below 15 seconds, `max_reconnect_backoff` is below `reconnect_backoff`,
                `max_outage` is negative or not finite, a timeout, backoff, interval, or count
                is not positive, only one of the two S3 credentials is set, `api_key` or an S3
                credential has leading or trailing whitespace (or an S3 credential is blank), a
                `ws_hosts` value is not a bare lower-case hostname, an environment variable
                read for a default has leading or trailing whitespace,
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
        object.__setattr__(self, "ws_hosts", _validate_ws_hosts(self.ws_hosts))
        basic = _validate_subscriptions(self.rest_basic_subscriptions, "rest_basic_subscriptions")
        object.__setattr__(self, "rest_basic_subscriptions", basic)
        delayed = _validate_subscriptions(self.delayed_subscriptions, "delayed_subscriptions")
        _check_delayed_subscriptions(delayed, self.ws_hosts)
        object.__setattr__(self, "delayed_subscriptions", delayed)

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
