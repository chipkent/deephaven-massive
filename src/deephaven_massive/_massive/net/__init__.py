"""What every Massive wire client shares: errors, TLS and proxy settings, JSON decoding.

`net` is the bottom layer of `deephaven_massive._massive`. It imports only the standard library
and `certifi` (plus `orjson` when installed), never another part of `_massive`, and the REST,
WebSocket, and flat-file clients all build on it:

- `net.errors`: `WireError`, `WireAuthError`, `WireEntitlementError`, re-exported here.
- `net.http`: the CA bundle and the proxy each client uses.
- `net.codec`: the JSON decoder (orjson when installed, else a strict standard-library path).
- `net.retry`: the retry schedule, the `Retry-After` parser, TLS-failure detection, and the
  injectable `Clock` every wait goes through.

Attributes:
    USER_AGENT: The `User-Agent` every client sends, `deephaven-massive/<version>`, where the
        version is the installed distribution's (for example `"deephaven-massive/0.1.0"`).
"""

from __future__ import annotations

from importlib.metadata import version

from deephaven_massive._massive.net.errors import WireAuthError, WireEntitlementError, WireError

__all__ = ["USER_AGENT", "WireAuthError", "WireEntitlementError", "WireError"]

USER_AGENT: str = f"deephaven-massive/{version('deephaven-massive')}"
