"""URL origins: which scheme, host, and port a URL reaches.

Two places compare origins: the REST client, which follows a `next_url` only on its own base
URL's origin, and the library, which shares one rate governor between the sessions whose API
key and REST origin are the same. Both use `origin_of`, so two spellings of one address (an
upper-case host, a default port written out) always compare equal.

The function is pure, so it is safe from any thread. This module imports only the standard
library, so naming it loads no client.
"""

from __future__ import annotations

from typing import Final
from urllib.parse import urlsplit

from deephaven_massive._massive.net.redaction import without_userinfo_or_query

__all__ = ["Origin", "origin_of"]

_DEFAULT_PORTS: Final = {"http": 80, "https": 443}

Origin = tuple[str, str, int]
"""A URL's scheme, lower-case host name, and port (the scheme's default when not written)."""


def origin_of(url: str) -> Origin:
    """Return the origin of an absolute URL, for comparing where two URLs point.

    Args:
        url: An absolute `http` or `https` URL, such as `"https://api.massive.com"`.

    Returns:
        `(scheme, host, port)`: the scheme and host lower-cased, as `urlsplit` gives them,
        and the port as written (even 0), or the scheme's default when none is written.

    Raises:
        ValueError: If `url` is not an absolute `http` or `https` URL with a host, or its port
            is not a number.
    """
    parts = urlsplit(url)
    if parts.scheme not in _DEFAULT_PORTS or not parts.hostname:
        raise ValueError(f"{without_userinfo_or_query(url)!r} is not an absolute http or https URL")
    port = _DEFAULT_PORTS[parts.scheme] if parts.port is None else parts.port
    return parts.scheme, parts.hostname, port
