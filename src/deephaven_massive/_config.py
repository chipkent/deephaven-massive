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
from dataclasses import dataclass, field
from datetime import time
from typing import TYPE_CHECKING, Final

from deephaven_massive._enums import (
    AssetClass,
    OnError,
    Subscription,
    _require_enum,
    subscription_of,
)
from deephaven_massive._errors import AuthenticationError
from deephaven_massive._types import override

if TYPE_CHECKING:
    from collections.abc import Iterator

__all__ = [
    "MIN_RECONNECT_BACKOFF",
    "HostMap",
    "MassiveConfig",
    "environment_text",
    "require_positive_int",
    "require_positive_number",
]

MIN_RECONNECT_BACKOFF: Final = 15.0
"""The shortest `reconnect_backoff` accepted, in seconds.

Massive releases a dropped connection only after roughly 10 to 30 seconds; a client that
retries sooner can be refused for holding the allowance it just lost.
"""

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
    """Everything a `MassiveSession` needs to know: credentials, hosts, timeouts, and limits.

    Construct it with keyword arguments; any field left out takes its default, and the
    credentials default to the environment. The object is frozen and validated at
    construction, so it can be shared between sessions and threads. Secrets are left out of
    its `repr`.

    Example:
        ::

            import deephaven_massive as dm

            cfg = dm.MassiveConfig()  # MASSIVE_API_KEY from the environment
            pinned = dm.MassiveConfig(
                ws_hosts={dm.AssetClass.STOCKS: "delayed.massive.com"},
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
        ws_hosts: Pins the WebSocket hostname of the TICK channel family of an asset class,
            for example `{dm.AssetClass.STOCKS: "delayed.massive.com"}`, instead of letting
            the library decide from REST evidence. Hostnames only: no scheme, port, or path.
            The FMV family is never pinned; it is always `business.massive.com`. Defaults to
            an empty mapping: the library decides every host.
        ack_timeout: Seconds to wait for a subscription acknowledgement before re-sending it
            once. Defaults to 5 seconds.
        reconnect_backoff: Seconds of the first wait before the WebSocket client tries again
            after any failure that is not fatal: a connection that opened and closed (cleanly
            or not, including a `max_connections` refusal), a TCP or TLS failure, a handshake
            or authentication timeout, or a retryable HTTP status during the upgrade (500,
            502, 503, 504). Each further consecutive failure doubles it, up to
            `max_reconnect_backoff`, and the count resets only once a connection has stayed
            up for 60 seconds. Must be at least `MIN_RECONNECT_BACKOFF` (15 s): Massive's
            knowledge base says the server takes "roughly 10 to 30 seconds" to release a
            dropped connection, and an earlier retry can be refused for holding the allowance
            it just lost, which on an individual plan wastes the one connection there is. The
            30-second default waits out that whole window.
        max_reconnect_backoff: Cap, in seconds, of the doubling reconnect backoff. Jitter only
            lengthens a wait, so every wait lies in `[reconnect_backoff,
            max_reconnect_backoff]`. Defaults to 120 seconds.
        flat_file_symbol_threshold: A count of symbols. With `HistorySource.AUTO`, when both
            flat files and REST can serve a request, this many symbols or more (or `dm.ALL`)
            read flat files. Defaults to 50.
        flat_file_publish_cutoff: A New York wall-clock time, a `datetime.time` without a
            timezone. Before it, yesterday's flat file is treated as not yet published (files
            land around 11:00 ET). Defaults to 11:30.
        max_concurrency: Maximum parallel REST requests of one `poll()` fan-out, and parallel
            S3 reads: a count of threads. Defaults to 8.
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
        flush_interval: Seconds between WebSocket micro-batch flushes into the live tables.
            Defaults to 0.05 seconds (50 ms).
        rest_base_url: The REST API base URL. Defaults to `https://api.massive.com`.
        s3_endpoint_url: The flat-file S3 endpoint URL. Defaults to
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
    flat_file_symbol_threshold: int = 50
    flat_file_publish_cutoff: time = time(11, 30)
    max_concurrency: int = 8
    rest_basic_subscriptions: AbstractSet[Subscription] = frozenset()
    flush_interval: float = 0.05
    rest_base_url: str = "https://api.massive.com"
    s3_endpoint_url: str = "https://files.massive.com"
    s3_bucket: str = "flatfiles"
    noncanonical_symbols: OnError = field(default_factory=_env_noncanonical_symbols)
    unreadable_symbols: OnError = field(default_factory=_env_unreadable_symbols)

    def __post_init__(self) -> None:
        """Validate every field; freeze `ws_hosts` and `rest_basic_subscriptions` into copies.

        A `noncanonical_symbols` or `unreadable_symbols` that is not a `dm.OnError`, a
        `ws_hosts` key that is not a
        `dm.AssetClass`, a host that is not a `str`, a `rest_basic_subscriptions` that is not a
        set of `dm.Subscription`, or a numeric field of a non-numeric type also raises
        `TypeError`, and a non-positive number `ValueError` (`require_positive_number`,
        `require_positive_int`).

        Raises:
            TypeError: If `api_key` is not a `str` or `flat_file_publish_cutoff` is not a
                `datetime.time`.
            AuthenticationError: If no API key was given and none is in the environment.
            ValueError: If `reconnect_backoff` is below `MIN_RECONNECT_BACKOFF`,
                `max_reconnect_backoff` is below `reconnect_backoff`, a timeout,
                backoff, interval, or count is not positive,
                `flat_file_publish_cutoff` carries a timezone, only one of the two S3
                credentials is set, `api_key` or an S3 credential has leading or trailing
                whitespace (or an S3 credential is blank), a `ws_hosts` value is not a bare
                lower-case hostname, or an environment variable read for a default has
                leading or trailing whitespace (`environment_text`).
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
        if self.max_reconnect_backoff < self.reconnect_backoff:
            raise ValueError("max_reconnect_backoff must not be below reconnect_backoff")
        cutoff: object = self.flat_file_publish_cutoff  # checked at run time, whatever the hint
        if not isinstance(cutoff, time):
            kind = type(cutoff).__name__
            raise TypeError(f"flat_file_publish_cutoff must be a datetime.time, not {kind}")
        if cutoff.tzinfo is not None:
            raise ValueError("flat_file_publish_cutoff is a New York wall-clock time; drop tzinfo")
        _require_enum(self.noncanonical_symbols, OnError, "noncanonical_symbols")
        _require_enum(self.unreadable_symbols, OnError, "unreadable_symbols")
        object.__setattr__(self, "ws_hosts", _validate_ws_hosts(self.ws_hosts))
        basic = _validate_basic_subscriptions(self.rest_basic_subscriptions)
        object.__setattr__(self, "rest_basic_subscriptions", basic)


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


def _validate_basic_subscriptions(value: object) -> frozenset[Subscription]:
    """Validate `rest_basic_subscriptions` and return it as a frozen set.

    Args:
        value: The value passed as `MassiveConfig.rest_basic_subscriptions`.

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
            "rest_basic_subscriptions must be a set of dm.Subscription, such as "
            f"{{dm.Subscription.STOCKS}}, not {type(value).__name__}"
        )
    for item in value:
        if (
            isinstance(item, str)
            and not isinstance(item, Subscription)
            and item.casefold() in _CURRENCY_CLASSES
        ):
            raise TypeError(
                "rest_basic_subscriptions must hold dm.Subscription members; forex and crypto "
                "are one subscription, dm.Subscription.CURRENCIES"
            )
        _require_enum(item, Subscription, "each rest_basic_subscriptions member")
    return frozenset(value)


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
                f"'delayed.massive.com', got {value!r}"
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
