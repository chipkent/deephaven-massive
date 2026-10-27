"""`MassiveConfig`, the session's immutable configuration, and the JSON codec choice.

`MassiveConfig` is the only way to configure the library: `MassiveSession(MassiveConfig(...))`.
Every default lives in exactly one place, the field defaults of the dataclass below; nothing else in
the package repeats them. `MassiveConfig()` with no arguments reads the API key and the S3
credentials from the environment and validates everything at construction, so a bad configuration
fails before any session exists.

What is deliberately *not* configurable: the options contract cap (1,000 per connection), the
evidence endpoints of the host decision, the plan tier (Massive offers no entitlement endpoint,
so the library learns the tier from REST evidence instead), and the JSON codec. The codec is
`orjson` when the optional extra `deephaven-massive[orjson]` is installed and the standard
library otherwise; `json_codec()` reports the choice and the session logs it once at INFO.
"""

from __future__ import annotations

import importlib
import json
import logging
import math
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import time
from typing import TYPE_CHECKING, Final, Protocol

from deephaven_massive._enums import AssetClass, _require_enum
from deephaven_massive._errors import AuthenticationError, log_once
from deephaven_massive._types import override

if TYPE_CHECKING:
    from collections.abc import Iterator

__all__ = [
    "MIN_RECONNECT_BACKOFF",
    "HostMap",
    "JsonCodec",
    "MassiveConfig",
    "json_codec",
    "log_json_codec_once",
    "require_positive_int",
    "require_positive_number",
]

logger = logging.getLogger(__name__)

MIN_RECONNECT_BACKOFF: Final = 15.0
"""The shortest `reconnect_backoff` accepted, in seconds.

Massive releases a dropped connection only after roughly 10 to 30 seconds; a client that
retries sooner can be refused for holding the allowance it just lost.
"""

_HOSTNAME: Final = re.compile(
    r"^(?=.{1,253}$)[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?(\.[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?)*$"
)


def _env(*names: str) -> str | None:
    """Return the first non-empty environment variable among `names`.

    Args:
        *names: Variable names, in priority order.

    Returns:
        The value, or None when every variable is unset or empty.
    """
    for name in names:
        value = os.environ.get(name, "").strip()
        if value:
            return value
    return None


def _env_api_key() -> str:
    """Read the API key from `MASSIVE_API_KEY`, then `POLYGON_API_KEY`.

    Returns:
        The key, or an empty string when neither is set (rejected by validation).
    """
    return _env("MASSIVE_API_KEY", "POLYGON_API_KEY") or ""


def _env_s3_access_key_id() -> str | None:
    """Read `MASSIVE_S3_ACCESS_KEY_ID`.

    Returns:
        The S3 access key id, or None.
    """
    return _env("MASSIVE_S3_ACCESS_KEY_ID")


def _env_s3_secret_access_key() -> str | None:
    """Read `MASSIVE_S3_SECRET_ACCESS_KEY`.

    Returns:
        The S3 secret access key, or None.
    """
    return _env("MASSIVE_S3_SECRET_ACCESS_KEY")


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
                ws_hosts={dm.AssetClass.STOCKS: "fullmarket-business.massive.com"},
                max_concurrency=4,
            )
            m = dm.MassiveSession(pinned)

    Attributes:
        api_key: The Massive API key. Defaults to `MASSIVE_API_KEY`, then `POLYGON_API_KEY`.
        s3_access_key_id: The flat-file S3 access key id. Defaults to
            `MASSIVE_S3_ACCESS_KEY_ID`. Flat-file history needs both S3 credentials.
        s3_secret_access_key: The flat-file S3 secret. Defaults to
            `MASSIVE_S3_SECRET_ACCESS_KEY`.
        ws_hosts: Pins the WebSocket hostname of the TICK channel family of an asset class,
            for example `{dm.AssetClass.STOCKS: "fullmarket-business.massive.com"}`, instead
            of letting the library decide from REST evidence. Hostnames only: no scheme, port,
            or path. The FMV family is never pinned; it is always `business.massive.com`.
        ack_timeout: Seconds to wait for a subscription acknowledgement before re-sending it
            once.
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
            max_reconnect_backoff]`.
        flat_file_symbol_threshold: With `HistorySource.AUTO`, when both flat files and REST
            can serve a request, this many symbols or more (or `dm.ALL`) read flat files.
        flat_file_publish_cutoff: A New York wall-clock time. Before it, yesterday's flat file
            is treated as not yet published (files land around 11:00 ET).
        max_concurrency: Maximum parallel REST requests of one `poll()` fan-out, and parallel
            S3 reads.
        flush_interval: Seconds between WebSocket micro-batch flushes into the live tables.
        rest_base_url: The REST API base URL.
        s3_endpoint_url: The flat-file S3 endpoint URL.
        s3_bucket: The flat-file S3 bucket.
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
    flush_interval: float = 0.05
    rest_base_url: str = "https://api.massive.com"
    s3_endpoint_url: str = "https://files.massive.com"
    s3_bucket: str = "flatfiles"

    def __post_init__(self) -> None:
        """Validate every field and freeze `ws_hosts` into a read-only, lower-cased copy.

        A `ws_hosts` key that is not a `dm.AssetClass`, a host that is not a `str`, or a
        numeric field of a non-numeric type also raises `TypeError`, and a non-positive number
        `ValueError` (`require_positive_number`, `require_positive_int`).

        Raises:
            TypeError: If `api_key` is not a `str` or `flat_file_publish_cutoff` is not a
                `datetime.time`.
            AuthenticationError: If no API key was given and none is in the environment.
            ValueError: If `reconnect_backoff` is below `MIN_RECONNECT_BACKOFF`,
                `max_reconnect_backoff` is below `reconnect_backoff`, a timeout,
                backoff, interval, or count is not positive,
                `flat_file_publish_cutoff` carries a timezone, only one of the two S3
                credentials is set, or a `ws_hosts` value is not a bare hostname.
        """
        if not isinstance(self.api_key, str):
            raise TypeError(f"api_key must be a str, not {type(self.api_key).__name__}")
        if not self.api_key.strip():
            raise AuthenticationError(
                "no Massive API key: pass MassiveConfig(api_key=...) or set MASSIVE_API_KEY"
            )
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
        object.__setattr__(self, "ws_hosts", _validate_ws_hosts(self.ws_hosts))


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


def _validate_ws_hosts(ws_hosts: object) -> Mapping[AssetClass, str]:
    """Validate the `ws_hosts` mapping and return a read-only, normalized copy.

    Args:
        ws_hosts: The value passed as `MassiveConfig.ws_hosts`.

    Returns:
        A read-only mapping with lower-cased hostnames.

    Raises:
        TypeError: If `ws_hosts` is not a mapping, a key is not a `dm.AssetClass`, or a value
            is not a `str`.
        ValueError: If a value is not a bare hostname (a scheme, port, or path is present).
    """
    if not isinstance(ws_hosts, Mapping):
        raise TypeError("ws_hosts must be a mapping of dm.AssetClass to hostname")
    hosts: dict[AssetClass, str] = {}
    for key, value in ws_hosts.items():
        asset_class = _require_enum(key, AssetClass, "ws_hosts key")
        if not isinstance(value, str):
            raise TypeError(f"ws_hosts[{asset_class}] must be a hostname str")
        host = value.strip().lower()
        if not _HOSTNAME.match(host):
            raise ValueError(
                f"ws_hosts[{asset_class}] must be a bare hostname such as "
                f"'delayed.massive.com', got {value!r}"
            )
        hosts[asset_class] = host
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


class JsonCodec(Protocol):
    """A JSON decoder: `orjson.loads` or `json.loads`."""

    def __call__(self, data: str | bytes, /) -> object:
        """Decode one JSON document.

        Args:
            data: The document's text or UTF-8 bytes.

        Returns:
            The decoded value.
        """


def _select_json_codec() -> tuple[str, JsonCodec]:
    """Pick the JSON decoder: `orjson` when it can be imported, otherwise the standard library.

    `orjson` is an optional extra because it publishes no free-threaded wheels.

    Returns:
        `("orjson", orjson.loads)` or `("json", json.loads)`.
    """
    try:
        orjson = importlib.import_module("orjson")
    except ImportError:
        return "json", json.loads
    loads: JsonCodec = orjson.loads
    return "orjson", loads


_CODEC: Final = _select_json_codec()


def json_codec() -> tuple[str, JsonCodec]:
    """Return the process's JSON decoder and its name.

    The WebSocket layer decodes every frame exactly once with this codec. The SDK client keeps
    the standard library for the frames it sends, because `orjson.dumps` returns bytes.

    Returns:
        `(name, loads)`, where `name` is `"orjson"` or `"json"`.
    """
    return _CODEC


def log_json_codec_once() -> None:
    """Log the JSON codec choice at INFO, the first time any session starts in the process."""
    log_once(logger, "json-codec", "JSON codec: %s", _CODEC[0])
