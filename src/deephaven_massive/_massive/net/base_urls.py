"""REST base URLs: which URLs the REST client may send the API key to.

The REST client sends the API key as a bearer header on every request to its base URL, so a
base URL must be an origin alone: an `http` or `https` scheme, a host, and an optional port
other than 0, with no user and password, path, query, or fragment. Plain `http` would send the
key in clear text, so it is allowed only for a loopback host, such as a local test server.

`check_rest_base_url` is the one body of that rule. The REST client applies it when it is
created, and `MassiveConfig` applies it to `rest_base_url` when the config is built, so a URL
the client would refuse is refused at the config, naming the setting, and the two checks cannot
drift apart.

The functions are pure, so they are safe from any thread. This module imports only the
standard library and `net`, so naming it loads no client.
"""

from __future__ import annotations

import ipaddress
import logging
from urllib.parse import urlsplit

from deephaven_massive._massive.net.origins import Origin, origin_of
from deephaven_massive._massive.net.redaction import (
    without_userinfo,
    without_userinfo_or_query,
)

__all__ = ["check_rest_base_url"]

logger = logging.getLogger(__name__)


def _is_loopback(host: str) -> bool:
    """Return whether a host name is this machine: `localhost` or a loopback address.

    Args:
        host: A lower-case host name or IP address, without brackets.

    Returns:
        True for `localhost`, `127.0.0.0/8`, and `::1`.
    """
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _refusal(setting: str, base_url: str, reason: str) -> str:
    """Log a refused base URL at DEBUG and return the message of the error to raise.

    The caller raises the error, which the library logs at the level its cause gives, so the
    refusal logs here at DEBUG only.

    Args:
        setting: The name the caller knows the URL by, for the message.
        base_url: The refused URL; its user, password, query, and fragment never reach the
            message or the log (`without_userinfo_or_query`).
        reason: What is wrong with it, completing the message after the URL.

    Returns:
        A message naming the setting, the URL with `***` in place of a user and password, a
        query, and a fragment, and the reason.
    """
    message = f"{setting} {without_userinfo_or_query(base_url)!r} {reason}"
    logger.debug("REST base URL refused: %s", message)
    return message


def check_rest_base_url(base_url: str, *, setting: str) -> Origin:
    """Check a REST base URL and return its origin.

    Example:
        ::

            check_rest_base_url("https://api.massive.com", setting="rest_base_url")
            # ("https", "api.massive.com", 443)
            check_rest_base_url("https://api.massive.com/v3", setting="rest_base_url")
            # ValueError: rest_base_url 'https://api.massive.com/v3' must name only a
            # scheme, host, and port

    Args:
        base_url: The base URL, such as `"https://api.massive.com"`. A trailing `/` is allowed.
        setting: The name the caller knows the URL by, such as `"rest_base_url"`, which starts
            every message.

    Returns:
        The URL's `(scheme, host, port)`, the scheme's default port filled in.

    Raises:
        ValueError: If `base_url` is not an `http` or `https` URL with a host and a valid port
            (port 0, which no connection can reach, is not valid), names more than an origin (a
            user and password, a path, a query, or a fragment), or is `http` for a host that is
            not loopback, which would send the API key in clear text. The message shows the URL
            with `***` in place of a user and password, a query, and a fragment.
    """
    invalid = "is not an absolute http or https URL with a valid port"
    try:
        origin = origin_of(base_url)
    except ValueError:
        # Not chained: the parser's error can quote part of a password, as in the port error
        # of `https://user:s3cr3t?x@host`.
        raise ValueError(_refusal(setting, base_url, invalid)) from None
    if origin[2] == 0:  # the client would keep `:0` in every request URL and never connect
        raise ValueError(_refusal(setting, base_url, invalid))
    parts = urlsplit(base_url)
    if parts.path not in {"", "/"} or parts.query or parts.fragment or "@" in parts.netloc:
        raise ValueError(_refusal(setting, base_url, "must name only a scheme, host, and port"))
    if origin[0] == "http" and not _is_loopback(origin[1]):
        raise ValueError(
            _refusal(
                setting,
                base_url,
                "would send the API key in clear text; use https "
                "(http is allowed only for a loopback host, such as a local test server)",
            )
        )
    logger.debug("REST base URL %s %r reaches %s", setting, without_userinfo(base_url), origin)
    return origin
