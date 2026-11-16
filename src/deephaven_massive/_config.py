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

The settings file's reference, the generated part of `docs/configuration.md`, is rendered
here too, by `settings_reference`: an example file that sets every setting (`_EXAMPLE`, which
the tests load), a table of each setting's purpose, values, and default, with each
`websocket_hosts` key named by its full path, and each setting's advice on when to change it.
The text comes from the fields' defaults, the `Attributes` entries of the class docstring, and
each setting's value form (`_value_forms`, whose examples the tests load through the settings
file). It lives beside `_CONVERTERS` so that the page cannot drift from the code. The module
runs past 1,000 lines because all three parts are one subject, the settings: what they are, how
a file spells them, and how they are documented.

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
import inspect
import logging
import math
import os
import re
from collections.abc import Callable, Mapping
from collections.abc import Set as AbstractSet
from dataclasses import MISSING, asdict, dataclass, field, fields
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final, NamedTuple, Protocol, TypeVar

import yaml
from typing_extensions import override

from deephaven_massive._enums import (
    AssetClass,
    ConnectionTaken,
    OnError,
    Subscription,
    _require_enum,
    subscription_of,
)
from deephaven_massive._error_policy import ErrorPolicy, check_policy
from deephaven_massive._errors import AuthenticationError, ErrorSource
from deephaven_massive._massive import (
    check_api_key,
    check_rest_base_url,
    without_userinfo,
    without_userinfo_or_query,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

__all__ = [
    "CONFIG_FILE_VARIABLE",
    "DELAYED_HOST",
    "DELAYED_PREFIX",
    "FMV_HOST",
    "MIN_RECONNECT_BACKOFF",
    "NO_ACK_RESEND",
    "REALTIME_HOST",
    "SETTINGS_REFERENCE_END",
    "SETTINGS_REFERENCE_START",
    "HostMap",
    "MassiveConfig",
    "delayed_fix",
    "environment_text",
    "require_positive_int",
    "require_positive_number",
    "settings_reference",
    "shown_setting",
    "with_settings_reference",
]

logger = logging.getLogger(__name__)

CONFIG_FILE_VARIABLE: Final = "DEEPHAVEN_MASSIVE_CONFIG_FILE"
"""The environment variable `MassiveConfig.from_file()` reads the settings file's path from."""

MIN_RECONNECT_BACKOFF: Final = 15.0
"""The shortest `reconnect_backoff` accepted, in seconds.

It limits reconnect churn: a connection that keeps dropping, or that another program with the
same key keeps taking (`connection_taken`), reconnects at most this often.
"""

NO_ACK_RESEND: Final = 60.0
"""Seconds between rounds of re-sends of the subscriptions Massive has not acknowledged.

A round sends every such topic in one subscribe frame on the open socket, so it costs the other
streams nothing; one a minute stays well above the 15 s Massive asks between reconnects. Each
round awaits its answer for `subscription_confirmation_timeout`, which must therefore be shorter.
"""

REALTIME_HOST: Final = "socket.massive.com"
"""Massive's real-time WebSocket feed, and the one documented host of forex and crypto."""

DELAYED_HOST: Final = "delayed.massive.com"
"""Massive's delayed WebSocket feed: 15 minutes behind, 10 for futures."""

FMV_HOST: Final = "business.massive.com"
"""The one WebSocket host Massive documents for the fair-market-value (FMV) channels
(`vendor/massive/websocket/<class>/fair-market-value.md`), real-time only."""

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
    credentials default to the environment. Every value is checked when you create it, and
    none can change afterward, so one config can serve several sessions. Its `repr` leaves out
    your secrets.

    Example:
        ::

            import deephaven_massive as dm

            cfg = dm.MassiveConfig()  # MASSIVE_API_KEY from the environment
            # Real-time stocks; options from Massive's delayed feed.
            mixed = dm.MassiveConfig(delayed_subscriptions={dm.Subscription.OPTIONS})
            # Stocks streams always from Massive's real-time host, socket.massive.com; at
            # most 4 REST requests or flat-file reads at once.
            pinned = dm.MassiveConfig(
                websocket_hosts={dm.AssetClass.STOCKS: "socket.massive.com"},
                max_concurrency=4,
            )
            m = dm.MassiveSession(pinned)

    `MassiveConfig.from_file()` builds one from a settings file instead; see there. Every
    setting, with its settings-file spelling, its values, and its default, is on
    https://github.com/chipkent/deephaven-massive/blob/main/docs/configuration.md.

    Attributes:
        api_key: The Massive API key. Defaults to the `MASSIVE_API_KEY` environment variable;
            with neither, construction raises `AuthenticationError`. A key with a space, a line
            break, or a character outside ASCII raises `ValueError`, which never shows the key.
        s3_access_key_id: The flat-file S3 access key id. Defaults to the
            `MASSIVE_S3_ACCESS_KEY_ID` environment variable, else None. Flat-file history
            needs both S3 credentials; without them, `history()` cannot read flat files.
        s3_secret_access_key: The flat-file S3 secret. Defaults to the
            `MASSIVE_S3_SECRET_ACCESS_KEY` environment variable, else None.
        delayed_subscriptions: Lists the subscriptions whose data Massive serves you delayed, so
            their streams use Massive's delayed feed and their snapshots accept its `DELAYED`
            label. Defaults to an empty set.

            Massive labels such data `DELAYED` in its answers, and its delayed feed runs 15
            minutes late, or 10 for futures. Fair market value (FMV) streams always use
            `business.massive.com`, the one host Massive documents for them. Never list
            `dm.Subscription.CURRENCIES`, the one subscription for forex and crypto: Massive has
            no delayed feed for it.

            Leave a delayed subscription out, and a stream of its current data raises
            `EntitlementError`. Its message gives the line of Python to add. A `fetch()` or
            `poll()` table of it fails with the same message, and `fetch(wait=True)` raises it.
            `history()` and other past data don't depend on this setting.
        rest_basic_subscriptions: Lists the subscriptions on Massive's free Basic plan, so their
            requests keep to Basic's five a minute from the start. Defaults to an empty set.

            Each of the five subscriptions has a free Basic plan, and every subscription you
            haven't bought is on it: list each of those. Leave out each one you
            pay for, on any plan, Starter included: a listed subscription's requests are held to
            five a minute.

            A Basic subscription you leave out runs at full speed until Massive first refuses
            one of its requests for the rate limit. From then on, it keeps to five a minute. The
            limit is shared by every session in this server that uses your key.
            `m.rate_limits()` shows each subscription's pace.
        websocket_hosts: Streams an asset class from a WebSocket host you name instead of the one
            the library picks, such as `business.massive.com` for index values. Defaults to an empty
            mapping.

            Set `indices` to `business.massive.com` to stream index values when Massive serves
            your index values there. Massive's documentation lists no other host for them,
            delayed or real-time. Set
            nothing else unless Massive tells you to.

            Without an entry, a subscription listed in `delayed_subscriptions` streams from
            `delayed.massive.com`, and every other from `socket.massive.com`. Massive documents
            those two hosts for every other stream. An entry moves every stream of its asset
            class, except Fair Market Value (FMV) streams, which always use
            `business.massive.com`.

            An entry must agree with `delayed_subscriptions`. Give a class a host whose name
            starts with `delayed.` exactly when its subscription is listed there, and never give
            one to forex or crypto. Write a hostname alone, with no scheme, port, or path.
        subscription_confirmation_timeout: Sets how many seconds Massive has to confirm a stream's
            channel before the library asks again. Defaults to 5 seconds.

            Raise it on a slow network, where confirmations arrive late. It must be below 60
            seconds.
        reconnect_backoff: Sets the shortest wait, in seconds, before reconnecting a dropped stream.
            Defaults to 30 seconds.

            Lower it to reconnect sooner after a brief drop. It must be at least 15 seconds,
            which limits reconnect churn, and at most `max_reconnect_backoff`.

            Each failure in a row doubles the wait, up to `max_reconnect_backoff`, and the count
            resets once a connection stays up for 60 seconds. A failure no retry can fix, such as a
            rejected key, isn't retried.
        max_reconnect_backoff: Sets the longest wait, in seconds, before reconnecting a dropped
            stream. Defaults to 120 seconds.

            Lower it to retry more often during a long outage, or raise it to retry less often.
            It must be at least `reconnect_backoff`.
        connection_taken: Decides what a live table does when another connection with the same
            API key takes over its own. Defaults to `FAIL`.

            Massive allows your account one WebSocket connection per asset class, and keeps the
            newest. When another program, or a session with different streaming settings,
            connects with the same key, Massive closes this session's connection.

            - `dm.ConnectionTaken.FAIL` fails the live tables at once, with an error that names
              the time and the fix.
            - `dm.ConnectionTaken.RECONNECT` reconnects after the reconnect backoff, which takes
              the connection back. If the other program does the same, the two take turns, and
              each loses data while the other holds the connection.

            Sessions with the same key and streaming settings share one connection whatever
            this setting. See
            https://github.com/chipkent/deephaven-massive/blob/main/docs/troubleshooting.md#live-table-messages.
        max_outage: Sets how many seconds a live table waits out an outage or Massive's rate
            limits before it fails. Defaults to 300 seconds.

            Set `0` to fail at the first outage. Set a larger value to keep tables through
            longer ones. Each kind of table counts its own time:

            - A stream counts the time its connection is down.
            - A `poll()` table counts the time its polls keep failing or stop arriving.
            - A watchlist's `fetch()` or `history()` counts the time each symbol's reads keep
              failing with a temporary error. Then `m.symbol_reads()` shows that symbol
              `FAILED`. Delete its row and add it back to read it again.
            - Any other `fetch()` or `history()` without `wait=True` counts the time it waits
              out Massive's rate limits.

            While a stream, a `poll()` table, or a watchlist waits, its `m.streams()` row shows
            why in `Error`. A failed table stays failed, so call the method again for a new one.
            See
            https://github.com/chipkent/deephaven-massive/blob/main/docs/troubleshooting.md#live-table-messages.
        flat_file_symbol_threshold: Sets how many symbols in a list make a `history()` call read
            Massive's flat files instead of a request per symbol, when both can serve it. Defaults
            to 50.

            Lower it to read flat files for shorter lists, or raise it to keep them on requests.
            Each flat file holds a whole day of data, and a flat-file read downloads all of
            it, however few symbols you ask for.
            A call for this many symbols or more reads flat files when all of these hold:

            - `MASSIVE_S3_ACCESS_KEY_ID` and `MASSIVE_S3_SECRET_ACCESS_KEY` are set.
            - Massive publishes flat files for the dataset.
            - The call names no `source`.
            - For bars that take `adjusted`, the call passes `adjusted=False`.

            A `history()` of `dm.ALL` reads flat files under the same conditions. A `history()`
            of a live table reads REST whenever REST can serve it.
        max_concurrency: Sets how many REST requests the session runs at a time, and separately how
            many flat files it reads at a time. Defaults to 8.

            Raise it to run more at once, or lower it to open fewer connections. Both limits
            cover all the session's calls together. It also sets how many downloads of
            `fetch()` and `history()` run at a time, counting each symbol of a watchlist read:
            the others wait their turn, and `m.streams()` shows them `LOADING`. A Basic
            subscription still keeps to five requests a minute.
        flush_interval: Sets how many seconds pass between the batches of streamed rows added to
            live tables. Defaults to 0.05 seconds (50 ms).

            A shorter interval shows each row sooner, and a longer one adds more rows in each
            update.
        rest_base_url: Sets the URL REST requests go to. Defaults to `https://api.massive.com`.

            Change it only to reach a server other than Massive's, such as a local test server.
            The config refuses a URL with a path, a query, a fragment, a user and password, or
            port 0. It also refuses an `http` URL of a host that isn't on your own machine. The
            error shows `***` in place of the URL's query and password.
        s3_endpoint_url: Sets the URL flat files are read from. Defaults to
            `https://files.massive.com`.

            Change it only to reach a server other than Massive's.
        s3_bucket: Sets the S3 bucket flat files are read from. Defaults to `flatfiles`.

            Change it only with `s3_endpoint_url`.
        on_error: Decides what one bad symbol or row does: fail the table, or skip it and keep
            going. Defaults to `dm.ErrorPolicy()`, which leaves every cause at
            `dm.ErrorPolicy.DEFAULTS`.

            Set a cause to `dm.OnError.RAISE` when a table must not run past it.
            `help(dm.ErrorPolicy)` lists the causes. By default each logs, except
            `refused_venues`, which raises.

            A dataset method's `on_error=` overrides this policy for one call. A call that
            leaves it out, or passes None, uses this policy for every cause, and each cause
            unset here falls back to `dm.ErrorPolicy.DEFAULTS`.
    """

    api_key: str = field(default_factory=_env_api_key, repr=False)
    s3_access_key_id: str | None = field(default_factory=_env_s3_access_key_id)
    s3_secret_access_key: str | None = field(default_factory=_env_s3_secret_access_key, repr=False)
    websocket_hosts: Mapping[AssetClass, str] = field(default_factory=dict)
    subscription_confirmation_timeout: float = 5.0
    reconnect_backoff: float = 30.0
    max_reconnect_backoff: float = 120.0
    connection_taken: ConnectionTaken = ConnectionTaken.FAIL
    max_outage: float = 300.0
    flat_file_symbol_threshold: int = 50
    max_concurrency: int = 8
    rest_basic_subscriptions: AbstractSet[Subscription] = frozenset()
    delayed_subscriptions: AbstractSet[Subscription] = frozenset()
    flush_interval: float = 0.05
    rest_base_url: str = "https://api.massive.com"
    s3_endpoint_url: str = "https://files.massive.com"
    s3_bucket: str = "flatfiles"
    on_error: ErrorPolicy = field(default_factory=ErrorPolicy)

    def __post_init__(self) -> None:
        """Validate every field; freeze `websocket_hosts` and the two subscription sets into copies.

        An `on_error` that is not a `dm.ErrorPolicy`, a `connection_taken` that is not a
        `dm.ConnectionTaken`, a `websocket_hosts` key that is not a `dm.AssetClass`, a host,
        URL, or bucket that is not a `str`, a
        `rest_basic_subscriptions` or `delayed_subscriptions` that is not a set of
        `dm.Subscription`, or a numeric field of a non-numeric type raises `TypeError`; a
        number that is not positive raises `ValueError`, though `max_outage` may be zero.

        Raises:
            TypeError: If `api_key` is not a `str`.
            AuthenticationError: If no API key was given and none is in the environment.
            ValueError: If `subscription_confirmation_timeout` is not below 60 seconds,
                `reconnect_backoff` is below 15 seconds, `max_reconnect_backoff` is below
                `reconnect_backoff`, `max_outage` is negative or not finite, a timeout, backoff,
                interval, or count is not positive, only one of the two S3 credentials is set,
                `api_key` or an S3 credential has leading or trailing whitespace (or an S3
                credential is blank), `api_key` holds whitespace or a character outside
                visible ASCII, which no header can carry, `rest_base_url` or `s3_endpoint_url`
                is not an `http` or `https` URL with a host, `rest_base_url` names port 0 or
                more than a scheme, host, and port (a user and password, a path, or a query),
                or is `http` to a host other than this machine, `s3_bucket` is blank, a
                `websocket_hosts` value is not a bare lower-case hostname, an environment
                variable read for a default has leading or trailing whitespace,
                `delayed_subscriptions` holds `dm.Subscription.CURRENCIES`, or a
                `websocket_hosts` pin of a delayed host does not match `delayed_subscriptions`.
        """
        if not isinstance(self.api_key, str):
            raise TypeError(f"api_key must be a str, not {type(self.api_key).__name__}")
        if not self.api_key.strip():
            raise AuthenticationError(
                "no Massive API key: pass MassiveConfig(api_key=...) or set MASSIVE_API_KEY",
                source=ErrorSource.USER,
            )
        if self.api_key != self.api_key.strip():
            raise ValueError(
                "api_key has leading or trailing whitespace; pass the key without any (the key "
                "is not shown)"
            )
        check_api_key(self.api_key, setting="api_key")
        _check_s3_credential("s3_access_key_id", self.s3_access_key_id)
        _check_s3_credential("s3_secret_access_key", self.s3_secret_access_key)
        if (self.s3_access_key_id is None) != (self.s3_secret_access_key is None):
            raise ValueError(
                "flat files need both s3_access_key_id and s3_secret_access_key "
                "(MASSIVE_S3_ACCESS_KEY_ID and MASSIVE_S3_SECRET_ACCESS_KEY)"
            )
        for name in (
            "subscription_confirmation_timeout",
            "reconnect_backoff",
            "max_reconnect_backoff",
            "flush_interval",
        ):
            require_positive_number(name, getattr(self, name))
        for name in ("flat_file_symbol_threshold", "max_concurrency"):
            require_positive_int(name, getattr(self, name))
        if self.reconnect_backoff < MIN_RECONNECT_BACKOFF:
            raise ValueError(
                f"reconnect_backoff must be at least {MIN_RECONNECT_BACKOFF:g} s, not "
                f"{self.reconnect_backoff:g}, which limits reconnect churn"
            )
        if self.subscription_confirmation_timeout >= NO_ACK_RESEND:
            raise ValueError(
                f"subscription_confirmation_timeout must be below {NO_ACK_RESEND:g} s, not "
                f"{self.subscription_confirmation_timeout:g}: "
                f"unacknowledged subscriptions are re-sent every {NO_ACK_RESEND:g} s, and each "
                "re-send awaits its answer for subscription_confirmation_timeout"
            )
        if self.max_reconnect_backoff < self.reconnect_backoff:
            raise ValueError("max_reconnect_backoff must not be below reconnect_backoff")
        object.__setattr__(self, "max_outage", _check_max_outage(self.max_outage))
        check_policy(self.on_error)
        _require_enum(self.connection_taken, ConnectionTaken, "connection_taken")
        _check_url("rest_base_url", self.rest_base_url)
        check_rest_base_url(self.rest_base_url, setting="rest_base_url")
        _check_url("s3_endpoint_url", self.s3_endpoint_url)
        _check_bucket(self.s3_bucket)
        object.__setattr__(self, "websocket_hosts", _validate_websocket_hosts(self.websocket_hosts))
        basic = _validate_subscriptions(self.rest_basic_subscriptions, "rest_basic_subscriptions")
        object.__setattr__(self, "rest_basic_subscriptions", basic)
        delayed = _validate_subscriptions(self.delayed_subscriptions, "delayed_subscriptions")
        _check_delayed_subscriptions(delayed, self.websocket_hosts)
        object.__setattr__(self, "delayed_subscriptions", delayed)

    @classmethod
    def from_file(cls, path: str | os.PathLike[str] | None = None) -> MassiveConfig:
        """Build a configuration from a YAML settings file, for settings shared between scripts.

        The file is a mapping of setting names, the attribute names of this class, to values;
        a setting it leaves out takes its default. Precedence is code, then the file, then the
        defaults: override a setting of the file in code with `dataclasses.replace`. The credentials
        are never read from the file; they come from code or from their environment variables, as
        for `MassiveConfig()`. Every scalar is read as text and converted by the setting's own rule,
        so a ticker such as `ON` or `NO` stays text, and an enum word is taken only as spelled
        exactly (`raise`, not `Raise`). Every setting's spelling, values, and default, with an
        example file, are on
        https://github.com/chipkent/deephaven-massive/blob/main/docs/configuration.md.

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
                `MassiveConfig()` never reads a file.

        Returns:
            The validated configuration.

        Raises:
            ValueError: If `path` is not given and `DEEPHAVEN_MASSIVE_CONFIG_FILE` is unset or
                blank (or has surrounding whitespace); if the file is not UTF-8 or not valid
                YAML; if it holds more than one document, a top level that is not a mapping
                (an empty file included), a duplicate key, a tag (`!!python/...` or any
                other), an anchor or alias, lists and mappings nested more than 10 deep, a
                credential (the message names the environment variable to use and never
                shows the value), or an unknown setting (the message suggests the closest
                name, and shows at most the key's first 40 characters); if a value is empty
                or cannot be converted; or if the constructor refuses a value. Each message
                names the file, and the setting and its line and column where known, except
                one about an environment variable (a credential with surrounding whitespace,
                or one S3 credential without the other), which names the variable instead.
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
            `"MassiveConfig(s3_access_key_id=None, websocket_hosts={}, ...)"`, field by field.
        """
        shown = ", ".join(
            f"{f.name}={shown_setting(f.name, getattr(self, f.name))}"
            for f in fields(self)
            if f.repr
        )
        return f"MassiveConfig({shown})"


_URL_SETTINGS: Final[dict[str, Callable[[str], str]]] = {
    "rest_base_url": without_userinfo_or_query,
    "s3_endpoint_url": without_userinfo,
}
"""The `MassiveConfig` fields that hold a URL, which a user may write with credentials, and
how each is shown. `rest_base_url` may hold no query or fragment, so a value that has one is
refused, and its query can carry the API key (`?apiKey=<key>`): it is shown with them hidden
too. An S3 endpoint may hold a query, which is shown."""


def shown_setting(name: str, value: object) -> str:
    """Return the text a log line or `repr` shows for one `MassiveConfig` setting.

    Args:
        name: The field's name.
        value: Its value.

    Returns:
        The value's `repr`; for a URL setting, with `***@` in place of a user and password
        (`without_userinfo`), and for `rest_base_url` with `***` in place of a query and a
        fragment as well (`without_userinfo_or_query`).
    """
    show = _URL_SETTINGS.get(name)
    if show is not None and isinstance(value, str):
        return repr(show(value))
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
    delayed: frozenset[Subscription], websocket_hosts: Mapping[AssetClass, str]
) -> None:
    """Refuse a `delayed_subscriptions` Massive cannot serve, or one a host pin contradicts.

    A host whose name starts with `delayed.` is a delayed feed: pinning one is asking for
    delayed data, which only a listed subscription may do, and a listed subscription may pin
    nothing else.

    Args:
        delayed: The validated `delayed_subscriptions`.
        websocket_hosts: The validated `websocket_hosts`.

    Raises:
        ValueError: If `delayed` holds `dm.Subscription.CURRENCIES` (Massive has no delayed
            forex or crypto feed), or `websocket_hosts` pins a `delayed.` host for forex or crypto,
            or for a class whose subscription is not listed, or pins any other host for one
            that is. Each message names both settings and what to change.
    """
    if Subscription.CURRENCIES in delayed:
        raise ValueError(
            "delayed_subscriptions cannot hold dm.Subscription.CURRENCIES: Massive has no "
            "delayed forex or crypto feed, so their streams are always real-time"
        )
    for asset_class, host in websocket_hosts.items():
        subscription = subscription_of(asset_class)
        listed = subscription in delayed
        pins_delayed = host.startswith(DELAYED_PREFIX)
        if pins_delayed and subscription is Subscription.CURRENCIES:
            raise ValueError(
                f"websocket_hosts pins {asset_class} to {host}, but Massive has no delayed forex "
                "or crypto feed; remove the pin"
            )
        if pins_delayed and not listed:
            raise ValueError(
                f"websocket_hosts pins {asset_class} to the delayed feed {host}, but "
                f"delayed_subscriptions does not list dm.Subscription.{subscription.name}; "
                f"to use the delayed feed write {delayed_fix(subscription)}"
            )
        if listed and not pins_delayed:
            raise ValueError(
                f"delayed_subscriptions lists dm.Subscription.{subscription.name}, "
                f"whose streams use Massive's delayed feed {DELAYED_HOST}, but websocket_hosts "
                f"pins {asset_class} to {host}; remove one of the two"
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
            password, and a `rest_base_url` with `***` in place of a query and a fragment
            (`shown_setting`).
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


def _validate_websocket_hosts(websocket_hosts: object) -> Mapping[AssetClass, str]:
    """Validate the `websocket_hosts` mapping and return a read-only, normalized copy.

    Args:
        websocket_hosts: The value passed as `MassiveConfig.websocket_hosts`.

    Returns:
        A read-only mapping of the hostnames as passed.

    Raises:
        TypeError: If `websocket_hosts` is not a mapping, a key is not a `dm.AssetClass`, or a value
            is not a `str`.
        ValueError: If a value is not a bare lower-case hostname (a scheme, port, path,
            upper-case letter, or surrounding whitespace is present); nothing is cleaned, and
            a value that would be a hostname once trimmed and lower-cased names that form.
    """
    if not isinstance(websocket_hosts, Mapping):
        raise TypeError("websocket_hosts must be a mapping of dm.AssetClass to hostname")
    hosts: dict[AssetClass, str] = {}
    for key, value in websocket_hosts.items():
        asset_class = _require_enum(key, AssetClass, "websocket_hosts key")
        if not isinstance(value, str):
            raise TypeError(f"websocket_hosts[{asset_class}] must be a hostname str")
        if value != value.strip().lower() and _HOSTNAME.match(value.strip().lower()):
            raise ValueError(
                f"websocket_hosts[{asset_class}]={value!r} must be written exactly as a lower-case "
                f"hostname; write {value.strip().lower()!r}"
            )
        if not _HOSTNAME.match(value):
            raise ValueError(
                f"websocket_hosts[{asset_class}] must be a bare hostname such as "
                f"'delayed.massive.com', got {without_userinfo(value)!r}"
            )
        hosts[asset_class] = value
    return HostMap(hosts)


class HostMap(Mapping[AssetClass, str]):
    """The read-only, hashable `websocket_hosts` mapping of a `MassiveConfig`.

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


_NESTING_LIMIT: Final = 10
"""The deepest a settings file's lists and mappings may nest. A setting needs two levels at
most (the top-level mapping, and the `websocket_hosts` mapping or a subscriptions list in it);
PyYAML composes nested nodes by recursion, so a file nested hundreds deep would otherwise end
in a `RecursionError` instead of a message naming the line."""


class _SettingsLoader(yaml.BaseLoader):
    """PyYAML's pure-Python base loader, refusing tags, anchors, aliases, and deep nesting.

    The base loader resolves no implicit types, so every scalar is text: `ON`, `NO`, `Y`,
    `off`, and `0700` stay as written instead of becoming booleans or numbers. It is pure
    Python, so it neither depends on libyaml nor builds any object a tag names. The settings
    file is read only as a node tree (`compose_document`), never constructed, so the nodes
    keep their positions for messages. One loader reads one file, on the calling thread.
    """

    def __init__(self, text: str) -> None:
        """Prepare to read one file's text.

        Args:
            text: The file's content.
        """
        super().__init__(text)
        self._depth = 0

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
                (`&name`), carries an explicit tag (`!name`, `!!python/object`, ...), or is
                a list or mapping nested deeper than `_NESTING_LIMIT`.
        """
        peek: Callable[[], yaml.Event] = self.peek_event  # PyYAML's stubs leave it untyped
        event = peek()
        if isinstance(event, yaml.CollectionStartEvent) and self._depth >= _NESTING_LIMIT:
            raise _SettingsFileError(
                f"lists and mappings nest more than {_NESTING_LIMIT} deep here; a setting "
                "needs two levels at most",
                event.start_mark,
            )
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
        self._depth += 1
        try:
            return super().compose_node(parent, index)
        finally:
            self._depth -= 1


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


def _on_error(node: yaml.Node) -> ErrorPolicy:
    """Convert `on_error`: a mapping of cause to `raise`, `log`, or `ignore`.

    Args:
        node: The value's node, such as `{malformed_rows: raise}`.

    Returns:
        The policy, setting each cause the mapping names.

    Raises:
        _SettingsFileError: If the node is not a mapping, a key is not a cause or appears
            twice, or a setting is not one of the words (`_word`).
    """
    causes = [f.name for f in fields(ErrorPolicy)]
    if not isinstance(node, yaml.MappingNode):
        raise _SettingsFileError(
            "must be a mapping of cause to raise, log, or ignore, such as {malformed_rows: raise}",
            node.start_mark,
        )
    settings: dict[str, OnError] = {}
    for key, value in node.value:
        cause = _scalar(key, "a cause")
        if cause not in causes:
            raise _SettingsFileError(
                f"has no cause {cause[:_SHOWN_KEY_LENGTH]!r}; the causes are {', '.join(causes)}",
                key.start_mark,
            )
        if cause in settings:
            raise _SettingsFileError(f"sets {cause!r} twice", key.start_mark)
        settings[cause] = _word(value, OnError, f"an error setting for {cause}")
    return ErrorPolicy(**settings)


def _connection_taken(node: yaml.Node) -> ConnectionTaken:
    """Convert `connection_taken`'s word: `fail` or `reconnect` (`_word`).

    Args:
        node: The value's node.

    Returns:
        The `ConnectionTaken` member.
    """
    return _word(node, ConnectionTaken, "a connection_taken value")


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
    """Convert `websocket_hosts`: a mapping of asset-class word to hostname.

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
    "websocket_hosts": _hosts,
    "subscription_confirmation_timeout": _seconds,
    "reconnect_backoff": _seconds,
    "max_reconnect_backoff": _seconds,
    "connection_taken": _connection_taken,
    "max_outage": _seconds,
    "flat_file_symbol_threshold": _count,
    "max_concurrency": _count,
    "rest_basic_subscriptions": _subscriptions,
    "delayed_subscriptions": _subscriptions,
    "flush_interval": _seconds,
    "rest_base_url": _text,
    "s3_endpoint_url": _text,
    "s3_bucket": _text,
    "on_error": _on_error,
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


# --- The settings reference ----------------------------------------------------------------------

SETTINGS_REFERENCE_START: Final = (
    "<!-- The settings below are generated from MassiveConfig by scripts/render_reference.py. "
    "Edit the class's docstring, not this part of the page. -->"
)
"""The line before the generated part of `docs/configuration.md`."""

SETTINGS_REFERENCE_END: Final = "<!-- End of the generated settings. -->"
"""The line after the generated part of `docs/configuration.md`."""


class _Example(NamedTuple):
    """One setting's entry in the example settings file of `docs/configuration.md`."""

    value: object
    """The value the example sets, as `MassiveConfig` takes it."""
    comment: str
    """What the setting does, in one line, written above it as a comment."""


_EXAMPLE_ACCOUNT: Final = (
    "This file sets every setting, and a line marked `# the default` can be left out. Its "
    "other values suit an account whose stocks and options data Massive serves delayed, whose "
    "index values Massive serves on business.massive.com, and whose Currencies and Futures "
    "subscriptions are on the free Basic plan:"
)
"""The sentence before the example settings file: the account its values agree with."""

_EXAMPLE: Final[Mapping[str, _Example]] = {
    "delayed_subscriptions": _Example(
        frozenset({Subscription.STOCKS, Subscription.OPTIONS}),
        "Subscriptions whose data Massive serves you delayed, 15 minutes late.",
    ),
    "rest_basic_subscriptions": _Example(
        frozenset({Subscription.CURRENCIES, Subscription.FUTURES}),
        "Subscriptions on the free Basic plan, held to five requests a minute.",
    ),
    "max_outage": _Example(
        600.0,
        "Seconds a live table waits out an outage before it fails; 0 fails at once.",
    ),
    "flush_interval": _Example(
        0.05, "Seconds between the batches of streamed rows added to live tables."
    ),
    "flat_file_symbol_threshold": _Example(
        50, "Symbols in one history() call that make it read flat files, once your S3 keys are set."
    ),
    "max_concurrency": _Example(
        8, "REST requests at a time, and separately flat files read at a time."
    ),
    "on_error": _Example(
        ErrorPolicy(
            noncanonical_symbols=OnError.RAISE,
            unreadable_symbols=OnError.LOG,
            unavailable_symbols=OnError.LOG,
            unreadable_keys=OnError.LOG,
            unconvertible_values=OnError.LOG,
            malformed_rows=OnError.LOG,
            refused_venues=OnError.RAISE,
        ),
        "What each kind of problem does: raise, log, or ignore.",
    ),
    "websocket_hosts": _Example(
        {AssetClass.INDICES: "business.massive.com"},
        "The WebSocket host per asset class: index values stream only from business.massive.com.",
    ),
    "subscription_confirmation_timeout": _Example(
        5.0, "Seconds Massive has to confirm a stream's channel before the library asks again."
    ),
    "reconnect_backoff": _Example(
        30.0, "Shortest wait, in seconds, before reconnecting a dropped stream; at least 15."
    ),
    "max_reconnect_backoff": _Example(
        120.0, "Longest wait, in seconds, before reconnecting a dropped stream."
    ),
    "connection_taken": _Example(
        ConnectionTaken.FAIL,
        "When another program with this key takes a stream's connection: fail, or reconnect.",
    ),
    "rest_base_url": _Example(
        "https://api.massive.com", "Where REST requests go: change it only for another server."
    ),
    "s3_endpoint_url": _Example(
        "https://files.massive.com",
        "Where flat files are read from: change it only for another server.",
    ),
    "s3_bucket": _Example(
        "flatfiles", "The flat files' S3 bucket: change it only with s3_endpoint_url."
    ),
}
"""The example settings file: every setting a file may hold, each once, in the order of the
page's table, the ones a new user sets first. Its values agree with one account
(`_EXAMPLE_ACCOUNT`), and the tests load the file and compare each value with these. Its
`on_error` sets every cause, each under its line of `_CAUSE_COMMENTS`."""

_CAUSE_COMMENTS: Final[Mapping[str, str]] = {
    "noncanonical_symbols": "A watchlist value that isn't Massive's exact ticker.",
    "unreadable_symbols": "A symbol in Massive's data that the library can't read.",
    "unavailable_symbols": "A symbol Massive has no data for.",
    "unreadable_keys": "A row of Massive's data whose key the library can't read.",
    "unconvertible_values": "A row of Massive's data with a value the library can't read.",
    "malformed_rows": "A flat-file line that isn't a readable row.",
    "refused_venues": "A futures exchange you name that the plan lacks.",
}
"""The comment above each cause of `on_error` in the example settings file, keyed by cause."""


_ATTRIBUTE: Final = re.compile(r"^    (\w+): (.*)$")
"""One setting's first line in the `Attributes` section of `MassiveConfig`'s cleaned docstring."""

_ENTRY_INDENT: Final = " " * 8
"""The indent of an entry's later lines in the `Attributes` section of the cleaned docstring."""

_DEFAULT_SENTENCE: Final = re.compile(r" ?Defaults to [^.]*(?:\.[^ ][^.]*)*\.$")
"""An entry's last sentence when it gives the default, which the reference shows from the code.

A period followed by a character other than a space (`0.05`, `api.massive.com`) does not end
the sentence."""

_ENUM_IN_CODE: Final = re.compile(
    r"`dm\.(OnError|Subscription|AssetClass|ConnectionTaken)\.([A-Z]+)`"
)
"""An enum member as code spells it, such as `dm.OnError.RAISE`, which a settings file spells
`raise`."""

_DOCS_PAGE_URL: Final = re.compile(
    r"https://github\.com/chipkent/deephaven-massive/blob/main/docs/([\w-]+\.md)#([\w-]+)"
)
"""A docstring's full link to a section of another page under `docs/`, which the console opens
and the settings page, itself under `docs/`, writes as a relative link."""


def _docstring_entries(cls: type) -> dict[str, list[list[str]]]:
    """Read each setting's entry from the `Attributes` section of a class's docstring.

    Args:
        cls: `MassiveConfig`, or `ErrorPolicy` for the causes of `on_error`.

    Returns:
        Each entry's paragraphs, keyed by the setting's name: each paragraph is its lines,
        without the entry's indent, so a list item keeps its `- ` and its continuation lines
        their two extra spaces.
    """
    doc = inspect.cleandoc(cls.__doc__ or "")
    section = doc[doc.index("\nAttributes:\n") :].splitlines()[2:]
    entries: dict[str, list[list[str]]] = {}
    new_paragraph = False
    for line in section:
        match = _ATTRIBUTE.match(line)
        if match is not None:
            entries[match.group(1)] = [[match.group(2)]]
            new_paragraph = False
        elif line.strip():
            paragraphs = entries[next(reversed(entries))]
            if new_paragraph:
                paragraphs.append([])
                new_paragraph = False
            paragraphs[-1].append(line.removeprefix(_ENTRY_INDENT))
        else:
            new_paragraph = True
    return entries


def _entries() -> dict[str, list[list[str]]]:
    """Read every entry of the reference: each setting's, and each cause's by its path.

    Returns:
        The paragraphs of each entry of `MassiveConfig`'s docstring, keyed by the setting's
        name, then those of `ErrorPolicy`'s, keyed by the cause's path, such as
        `on_error.malformed_rows` (`_cause_path`).
    """
    causes = {_cause_path(name): entry for name, entry in _docstring_entries(ErrorPolicy).items()}
    return {**_docstring_entries(MassiveConfig), **causes}


def _setting_texts() -> dict[str, str]:
    """Read each setting's summary: the first paragraph of its docstring entry.

    The summary says what the setting does and gives its default. The paragraphs after it say
    when to change the setting and what each value does (`_setting_details`).

    Returns:
        Each setting's summary as one line, without its last sentence when that sentence gives
        the default, keyed by the setting's name or the cause's path (`_entries`).
    """
    return {
        name: _DEFAULT_SENTENCE.sub("", " ".join(" ".join(paragraphs[0]).split()))
        for name, paragraphs in _entries().items()
    }


def _setting_details() -> dict[str, list[str]]:
    """Read each setting's detail: the paragraphs of its docstring entry after the summary.

    Returns:
        Each setting's later paragraphs as Markdown, line breaks kept, keyed by its name or the
        cause's path (`_entries`).
    """
    return {
        name: ["\n".join(lines) for lines in paragraphs[1:]]
        for name, paragraphs in _entries().items()
    }


def _as_file_words(text: str) -> str:
    """Return docstring text with each enum member in a settings file's spelling.

    Args:
        text: Text from the docstring, such as ``"`dm.OnError.RAISE` raises"``.

    Returns:
        The text with each member written as its value, such as ``"`raise` raises"``.
    """
    enums: dict[str, type[StrEnum]] = {
        "OnError": OnError,
        "Subscription": Subscription,
        "AssetClass": AssetClass,
        "ConnectionTaken": ConnectionTaken,
    }
    return _ENUM_IN_CODE.sub(lambda match: f"`{enums[match[1]][match[2]].value}`", text)


def _as_page_links(text: str) -> str:
    """Return docstring text with each full link to a `docs/` page as a relative Markdown link.

    The link's text is its anchor in words, which is the section's heading.

    Args:
        text: Text from the docstring, with a link such as
            `https://github.com/.../blob/main/docs/troubleshooting.md#report-a-bug`.

    Returns:
        The text with each such link written as `[Report a bug](troubleshooting.md#report-a-bug)`.
    """
    return _DOCS_PAGE_URL.sub(
        lambda match: f"[{match[2].replace('-', ' ').capitalize()}]({match[1]}#{match[2]})", text
    )


def _spellings(members: Iterable[StrEnum]) -> str:
    """Return enum members' settings-file words as a list for a page: `` `a`, `b`, or `c` ``."""
    words = [f"`{member.value}`" for member in members]
    *rest, last = words
    if len(rest) == 1:  # two words take no comma
        return f"{rest[0]} or {last}"
    return ", ".join(rest) + ", or " + last


_HOSTS_KEY: Final = "websocket_hosts"
"""The setting whose value maps asset class to host; the page names each of its keys by its full
path."""


def _host_path(asset_class: AssetClass) -> str:
    """Return the full path of one `websocket_hosts` key, such as `websocket_hosts.indices`."""
    return f"{_HOSTS_KEY}.{asset_class.value}"


_POLICY_KEY: Final = "on_error"
"""The setting whose value is a mapping of cause to setting; the page names each cause by its
full path."""


def _cause_path(cause: str) -> str:
    """Return the full path of one cause of `on_error`, such as `on_error.malformed_rows`."""
    return f"{_POLICY_KEY}.{cause}"


def _value_forms() -> dict[str, str]:
    """Return what a settings file writes for each setting, within the range the class accepts.

    Each form's example value, after "such as" (or, for a list of words, its last word), is one
    that `MassiveConfig.from_file` accepts; the tests load every one.

    Returns:
        A sentence fragment per row of the page's table, keyed by the row's setting: each
        setting a file may hold, except that each key of `websocket_hosts` and each cause of
        `on_error` has its own row, named by its full path (`_host_path`, `_cause_path`). For
        example, ``A positive whole number, such
        as `4` `` for `max_concurrency`.
    """
    delayed = [member for member in Subscription if member is not Subscription.CURRENCIES]
    error_words = _spellings([OnError.LOG, OnError.IGNORE, OnError.RAISE])
    url = (
        "An `https` URL of a host alone, or `http` for your own machine, such as "
        "`https://api.massive.com`"
    )
    forms = {
        "delayed_subscriptions": f"A list of any of {_spellings(delayed)}, such as "
        "`[stocks]`, or `[]` for none",
        "rest_basic_subscriptions": f"A list of any of {_spellings(Subscription)}, such as "
        "`[options]`, or `[]` for none",
        "max_outage": "A number of seconds, `0` or more, such as `600`",
        "flush_interval": "A positive number of seconds, such as `0.1`",
        "flat_file_symbol_threshold": "A positive whole number, such as `100`",
        "max_concurrency": "A positive whole number, such as `4`",
        **{_cause_path(f.name): error_words for f in fields(ErrorPolicy)},
        "subscription_confirmation_timeout": f"A positive number of seconds below "
        f"{NO_ACK_RESEND:g}, such as `10`",
        "reconnect_backoff": f"A number of seconds from {MIN_RECONNECT_BACKOFF:g} to "
        "`max_reconnect_backoff`, such as `60`",
        "max_reconnect_backoff": "A number of seconds, at least `reconnect_backoff`, such as `300`",
        "connection_taken": _spellings([ConnectionTaken.FAIL, ConnectionTaken.RECONNECT]),
        "rest_base_url": url,
        "s3_endpoint_url": "An `http` or `https` URL, such as `https://files.massive.com`",
        "s3_bucket": "A bucket name, such as `flatfiles`",
    }
    for asset_class in AssetClass:
        subscription = subscription_of(asset_class)
        example = "business.massive.com" if asset_class is AssetClass.INDICES else REALTIME_HOST
        rule = (
            "never a `delayed.` host"
            if subscription is Subscription.CURRENCIES
            else "a `delayed.` host exactly when `delayed_subscriptions` lists "
            f"`{subscription.value}`"
        )
        forms[_host_path(asset_class)] = f"A lower-case hostname, such as `{example}`; {rule}"
    return forms


def _host_row(asset_class: AssetClass, form: str) -> str:
    """Return the table row of one `websocket_hosts` key, named by its full path.

    Args:
        asset_class: The key.
        form: Its value form (`_value_forms`).

    Returns:
        The Markdown row: the key's path, what it sets, the form, and the host used without it.
    """
    purpose = f"Sets the WebSocket host {asset_class.value} streams come from."
    subscription = subscription_of(asset_class)
    default = (
        f"`{REALTIME_HOST}`"
        if subscription is Subscription.CURRENCIES
        else f"`{DELAYED_HOST}` when `delayed_subscriptions` lists `{subscription.value}`, "
        f"else `{REALTIME_HOST}`"
    )
    return f"| `{_host_path(asset_class)}` | {purpose} | {form} | {default} |"


def _file_value(value: object) -> str:
    """Return a value as a settings file writes it on one line, such as `[]`, `log`, or `0.05`."""
    if isinstance(value, StrEnum):
        return value.value
    if isinstance(value, AbstractSet):
        return "[" + ", ".join(sorted(member.value for member in value)) + "]"
    if isinstance(value, Mapping):
        return "{" + ", ".join(f"{key.value}: {host}" for key, host in value.items()) + "}"
    if isinstance(value, float):
        return f"{value:g}"
    return str(value)


def _defaults() -> dict[str, object]:
    """Return each field's default, from the field itself, keyed by its name."""
    return {
        f.name: f.default if f.default_factory is MISSING else f.default_factory()
        for f in fields(MassiveConfig)
    }


def _example_file() -> str:
    """Render the example settings file: every setting of `_EXAMPLE`, each under its comment.

    A value equal to the setting's default is marked `# the default`. The `websocket_hosts`
    mapping is written in block form, one asset class per indented line, and so is `on_error`,
    each cause under its comment (`_CAUSE_COMMENTS`) and marked against `ErrorPolicy.DEFAULTS`.

    Returns:
        The file's YAML text.
    """
    defaults = _defaults()
    lines: list[str] = []
    for name, example in _EXAMPLE.items():
        if lines:
            lines.append("")
        lines.append(f"# {example.comment}")
        marker = "  # the default" if example.value == defaults[name] else ""
        if isinstance(example.value, ErrorPolicy):
            lines.append(f"{name}:")
            default = asdict(ErrorPolicy.DEFAULTS)
            for cause, setting in asdict(example.value).items():
                same = "  # the default" if setting == default[cause] else ""
                lines += [f"  # {_CAUSE_COMMENTS[cause]}", f"  {cause}: {setting.value}{same}"]
        elif isinstance(example.value, Mapping):
            lines.append(f"{name}:{marker}")
            lines.extend(f"  {key.value}: {host}" for key, host in example.value.items())
        else:
            lines.append(f"{name}: {_file_value(example.value)}{marker}")
    return "\n".join(lines) + "\n"


def settings_reference() -> str:
    """Render the generated part of `docs/configuration.md` from `MassiveConfig` itself.

    Three parts, each under its own heading and anchor, so the page cannot drift from the code:

    - the example settings file (`_example_file`), which sets every setting;
    - a table with one row per setting a file may hold, in `_EXAMPLE` order, and one per key of
      `websocket_hosts` and per cause of `on_error`, named by its full path: what the setting is
      for (the first paragraph of its docstring entry), the values a file may write
      (`_value_forms`), and its default (from the field's own default, or a cause's in
      `ErrorPolicy.DEFAULTS`);
    - per setting, and per cause of `on_error`, the rest of its docstring entry: when to change
      it and what each value does.

    Enum members in the docstring's text are shown as a file spells them (`_as_file_words`).

    Returns:
        The Markdown that goes between `SETTINGS_REFERENCE_START` and `SETTINGS_REFERENCE_END`.

    Raises:
        ValueError: If `_EXAMPLE` does not hold every setting a file may hold, or
            `_CAUSE_COMMENTS` every cause of `ErrorPolicy`.
    """
    if sorted(_EXAMPLE) != sorted(_CONVERTERS):
        raise ValueError(
            f"the settings reference lists {sorted(_EXAMPLE)}, not every file setting "
            f"once: {sorted(_CONVERTERS)}"
        )
    causes = asdict(ErrorPolicy.DEFAULTS)
    if sorted(_CAUSE_COMMENTS) != sorted(causes):
        raise ValueError(
            f"the example file comments on {sorted(_CAUSE_COMMENTS)}, not every cause once: "
            f"{sorted(causes)}"
        )
    texts = _setting_texts()
    details = _setting_details()
    forms = _value_forms()
    defaults = _defaults()
    lines = [
        '<a id="example-settings-file"></a>',
        "",
        "### Every setting in one file",
        "",
        _EXAMPLE_ACCOUNT,
        "",
        "```yaml",
        _example_file() + "```",
        "",
        '<a id="settings-table"></a>',
        "",
        "### Values and defaults",
        "",
        (
            f"A key inside the `{_HOSTS_KEY}` or `{_POLICY_KEY}` mapping is named by its full "
            f"path: `{_host_path(AssetClass.INDICES)}` is the `indices` key under "
            f"`{_HOSTS_KEY}`."
        ),
        "",
        "| Setting | What it's for | Values | Default |",
        "|---|---|---|---|",
    ]
    for name in _EXAMPLE:
        if name == _HOSTS_KEY:
            lines.extend(_host_row(c, forms[_host_path(c)]) for c in AssetClass)
            continue
        if name == _POLICY_KEY:
            for cause, setting in causes.items():
                path = _cause_path(cause)
                text = _as_file_words(texts[path])
                lines.append(f"| `{path}` | {text} | {forms[path]} | `{setting.value}` |")
            continue
        default = _file_value(defaults[name])
        text = _as_file_words(texts[name])
        lines.append(f"| `{name}` | {text} | {forms[name]} | `{default}` |")
    lines += [
        "",
        '<a id="setting-details"></a>',
        "",
        "### When to change each setting",
        "",
        "`help(dm.MassiveConfig)` shows the same text in the console, written for Python code.",
    ]
    for name in _EXAMPLE:
        entries = [name] + ([_cause_path(c) for c in causes] if name == _POLICY_KEY else [])
        for entry in entries:
            lines += ["", f"#### {entry}"]
            for paragraph in details[entry]:
                lines += ["", _as_page_links(_as_file_words(paragraph))]
    return "\n".join(lines) + "\n"


def with_settings_reference(page: str) -> str:
    """Return `docs/configuration.md` with its generated part replaced by `settings_reference()`.

    Args:
        page: The page's text. Everything outside its two markers is kept as written.

    Returns:
        The page as `scripts/render_reference.py` writes it.

    Raises:
        ValueError: If the page lacks `SETTINGS_REFERENCE_START`, or
            `SETTINGS_REFERENCE_END` after it.
    """
    start = page.find(SETTINGS_REFERENCE_START)
    end = page.find(SETTINGS_REFERENCE_END, max(start, 0))
    if start < 0 or end < 0:
        raise ValueError(
            "the configuration page needs the lines SETTINGS_REFERENCE_START and "
            "SETTINGS_REFERENCE_END around its generated part"
        )
    head = page[: start + len(SETTINGS_REFERENCE_START)]
    return f"{head}\n\n{settings_reference()}\n{page[end:]}"
