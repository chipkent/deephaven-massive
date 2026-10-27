"""The certificate authorities and the proxy every Massive wire client uses.

Each client reads its TLS and proxy settings from here once, when it is constructed, so a
corporate network configured through the usual environment variables works the same way for
REST (urllib3), WebSocket (websockets), and flat files (boto3):

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

The functions read the environment on every call and keep no state, so they are safe from
any thread. This module imports only the standard library and `certifi`.
"""

from __future__ import annotations

import base64
import os
import ssl
import urllib.request
from urllib.parse import unquote, urlsplit, urlunsplit

import certifi

__all__ = ["ca_file", "https_proxy", "proxy_credentials", "ssl_context"]


def ssl_context() -> ssl.SSLContext:
    """Return a new client TLS context that verifies certificates and host names.

    The REST client hands it to urllib3 and the WebSocket client to `websockets`. Each call
    builds a fresh context, so a caller may adjust its own without affecting another client.

    Returns:
        `ssl.create_default_context()` when `SSL_CERT_FILE` or `SSL_CERT_DIR` is set to a
        non-empty value (OpenSSL then loads those locations), otherwise a default context
        that trusts only `certifi`'s bundle. OpenSSL silently skips an `SSL_CERT_FILE` or
        `SSL_CERT_DIR` it cannot read, so a wrong path shows up as a certificate verification
        failure on the first connection, which the clients report naming `SSL_CERT_FILE`.
    """
    if os.environ.get("SSL_CERT_FILE") or os.environ.get("SSL_CERT_DIR"):
        return ssl.create_default_context()
    return ssl.create_default_context(cafile=certifi.where())


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
    themselves, so the other clients need not call this.

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
