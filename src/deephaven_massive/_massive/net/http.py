"""The certificate authorities and the proxy every Massive wire client uses.

Each client reads its TLS and proxy settings from here once (the REST and WebSocket clients
when they are constructed, the flat-file client when its first request creates its boto3
client), so a corporate network configured through the usual environment variables works the
same way for REST (urllib3), WebSocket (websockets), and flat files (boto3):

- **Certificate authorities.** When `SSL_CERT_FILE` or `SSL_CERT_DIR` is set, the clients
  trust what the system's OpenSSL trusts, which honours both variables. Otherwise they trust
  `certifi`'s bundle, so a Python without system certificates (the python.org macOS build, a
  slim container) still verifies Massive's certificates.
- **Proxy.** `HTTPS_PROXY` (or `https_proxy`) names the proxy for HTTPS traffic, and
  `NO_PROXY` lists the hosts that bypass it, read through `urllib.request` exactly as the
  standard library reads them. On macOS and Windows, with neither variable set, the system's
  proxy settings apply, again as `urllib.request` reads them. A user and password in the
  proxy URL are sent as `Proxy-Authorization` (`proxy_credentials` splits them out for
  urllib3, which would not send them itself) and never appear in a log or an error.

The functions read the environment on every call, so they are safe from any thread and see a
changed variable at once. The one piece of state is the cache of TLS contexts behind
`ssl_context`: one context per distinct certificate setting, built once per process under a
lock. This module imports only the standard library and `certifi`.
"""

from __future__ import annotations

import base64
import os
import ssl
import threading
import urllib.request
from typing import Final
from urllib.parse import unquote, urlsplit, urlunsplit

import certifi

__all__ = ["ca_file", "https_proxy", "proxy_credentials", "ssl_context"]


_ALPN_PROTOCOLS: Final = ("http/1.1",)
"""The application protocols offered in the TLS handshake: REST and the WebSocket upgrade are
both HTTP/1.1. urllib3 sets the same list on the context before each connection it opens, so
setting it once here keeps the shared context the same whichever client connects first."""

_CONTEXTS_LOCK: Final = threading.Lock()
"""Guards `_CONTEXTS`. A leaf lock: no other lock is taken while it is held."""

_CONTEXTS: Final[dict[tuple[str, str], ssl.SSLContext]] = {}
"""The TLS context built for each (`SSL_CERT_FILE`, `SSL_CERT_DIR`) pair; guarded by
`_CONTEXTS_LOCK`. An unset variable is the empty string, as `ssl_context` treats it."""


def ssl_context() -> ssl.SSLContext:
    """Return the process's client TLS context for the current certificate settings.

    The REST client hands it to urllib3 (for Massive and for an HTTPS proxy) and the
    WebSocket client to `websockets`. Building a context reads and parses a certificate
    bundle, a few milliseconds of CPU, so one context is built per process for each distinct
    pair of `SSL_CERT_FILE` and `SSL_CERT_DIR` values and returned to every later caller with
    the same pair. A configured context is safe to share between threads and connections, so
    callers must not reconfigure it; urllib3 writes the same verification mode and ALPN list
    on it before each connection, which changes nothing. The proxy settings never shape the
    context, so they are not part of the key.

    The first call for a pair builds the context under a module lock, reading the bundle from
    local disk while it is held, so concurrent first calls build exactly one. Every later call
    takes the lock only to look the context up. A context lives as long as the process: a
    certificate bundle edited in place at the same path is read again only by a new process.

    Returns:
        A context that verifies certificates and host names and offers ALPN `http/1.1`:
        `ssl.create_default_context()` when `SSL_CERT_FILE` or `SSL_CERT_DIR` is set to a
        non-empty value (OpenSSL then loads those locations), otherwise a default context that
        trusts only `certifi`'s bundle. OpenSSL silently skips an `SSL_CERT_FILE` or
        `SSL_CERT_DIR` it cannot read, so a wrong path shows up as a certificate verification
        failure on the first connection, which the clients report naming `SSL_CERT_FILE`.
    """
    settings = (os.environ.get("SSL_CERT_FILE", ""), os.environ.get("SSL_CERT_DIR", ""))
    with _CONTEXTS_LOCK:
        context = _CONTEXTS.get(settings)
        if context is None:
            if any(settings):
                context = ssl.create_default_context()
            else:
                context = ssl.create_default_context(cafile=certifi.where())
            context.set_alpn_protocols(_ALPN_PROTOCOLS)
            _CONTEXTS[settings] = context
        return context


def ca_file() -> str:
    """Return the path of the certificate bundle for a client that takes a file, not a context.

    boto3 accepts only a bundle file (its `verify` argument), so the flat-file client uses this
    instead of `ssl_context()`. `SSL_CERT_DIR` names a directory of hashed certificates, which
    boto3 cannot read, so it does not apply here.

    Returns:
        The value of `SSL_CERT_FILE` when it is set and non-empty, otherwise the path of
        `certifi`'s bundle.
    """
    return os.environ.get("SSL_CERT_FILE") or certifi.where()


def https_proxy(host: str) -> str | None:
    """Return the proxy URL for HTTPS requests to `host`, or `None` to connect directly.

    The REST client builds a urllib3 `ProxyManager` when this returns a URL (urllib3 never
    reads the environment itself), and the flat-file client passes it to botocore's
    `Config(proxies=...)`. The WebSocket client does not call it: `websockets` reads the same
    variables itself.

    Args:
        host: The host name the client connects to, without scheme or port, such as
            `"api.massive.com"`.

    Returns:
        The HTTPS proxy URL from `urllib.request.getproxies()`, such as
        `"http://proxy.corp:3128"`, or `None` when no HTTPS proxy is configured or
        `urllib.request.proxy_bypass(host)` says `host` bypasses it (`NO_PROXY`).
    """
    proxy = urllib.request.getproxies().get("https")
    if not proxy or urllib.request.proxy_bypass(host):
        return None
    return proxy


def proxy_credentials(proxy_url: str) -> tuple[str, dict[str, str]]:
    """Split a proxy URL into the URL without credentials and the header that carries them.

    A corporate proxy often takes a user and password in the URL
    (`http://alice:secret@proxy.corp:3128`). urllib3's `ProxyManager` parses them but never
    sends them, so the REST client passes the returned header as its `proxy_headers`, and the
    URL without credentials as the proxy, which keeps the password out of every object a log
    or an error might show. botocore and `websockets` read credentials from the URL
    themselves; the flat-file client calls this only for the URL without credentials, to name
    the proxy in its messages.

    Args:
        proxy_url: The proxy URL, from `https_proxy`. A percent-encoded user or password
            (`p%40ss` for `p@ss`) is decoded.

    Returns:
        The proxy URL with no user information, and `{"Proxy-Authorization": "Basic <...>"}`
        (UTF-8 `user:password` in base64) when the URL names a user, or `{}` when it does not.
    """
    parts = urlsplit(proxy_url)
    if parts.username is None:
        return proxy_url, {}
    bare = urlunsplit(parts._replace(netloc=parts.netloc.rpartition("@")[2]))
    pair = f"{unquote(parts.username)}:{unquote(parts.password or '')}"
    token = base64.b64encode(pair.encode()).decode("ascii")
    return bare, {"Proxy-Authorization": f"Basic {token}"}
