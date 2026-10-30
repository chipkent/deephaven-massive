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
`ssl_context`: one context per consumer and certificate setting, built once per process
under a lock. This module imports only the standard library and `certifi`.
"""

from __future__ import annotations

import base64
import enum
import os
import ssl
import threading
import urllib.request
from pathlib import Path
from typing import Final
from urllib.parse import unquote, urlsplit, urlunsplit

import certifi

__all__ = ["TlsConsumer", "ca_file", "https_proxy", "proxy_credentials", "ssl_context"]


class TlsConsumer(enum.Enum):
    """The library a TLS context is built for; each gets its own shared contexts.

    urllib3 writes its verification mode and ALPN list onto the context it is given before
    every connection it opens. Keeping urllib3's contexts apart from those of `websockets`
    means those writes never touch a context a WebSocket handshake is using.
    """

    URLLIB3 = "urllib3"
    """The REST client's pools, and the refresh's downloads."""

    WEBSOCKETS = "websockets"
    """The WebSocket client's connections."""


_ALPN_PROTOCOLS: Final = ("http/1.1",)
"""The application protocols offered in the TLS handshake: REST and the WebSocket upgrade are
both HTTP/1.1, and urllib3 sets the same list before each connection anyway."""

_CONTEXTS_LOCK: Final = threading.Lock()
"""Guards `_CONTEXTS`. A leaf lock: no other lock is taken while it is held."""

_CONTEXTS: Final[dict[tuple[object, ...], ssl.SSLContext]] = {}
"""The TLS context built for each consumer and certificate setting (see `ssl_context`);
guarded by `_CONTEXTS_LOCK`."""


def ssl_context(consumer: TlsConsumer) -> ssl.SSLContext:
    """Return the process's client TLS context for `consumer` and the current settings.

    The REST client hands it to urllib3 (for Massive and for an HTTPS proxy) and the
    WebSocket client to `websockets`. Building a context reads and parses a certificate
    bundle, a few milliseconds of CPU, so contexts are shared: one is built per process for
    each consumer and each certificate setting, and returned to every later caller with the
    same pair. A configured context is safe to share between threads and connections (CPython
    guards each context's OpenSSL state with its own lock), so callers must not reconfigure
    it; urllib3 writes the same verification mode and ALPN list on its own contexts before
    each connection, which changes nothing.

    The certificate setting is everything that shapes the context: the values of
    `SSL_CERT_FILE`, `SSL_CERT_DIR`, and `SSLKEYLOGFILE`; the identity (modification time,
    size, and inode, or nothing when the path does not exist) of the file `SSL_CERT_FILE`
    names; and, for each directory in `SSL_CERT_DIR` (several are separated by `os.pathsep`,
    as OpenSSL reads them), the identity of the directory and the name of every entry in it
    with the identity of the file the entry resolves to (following symbolic links). Editing a
    file in place leaves its directory's own identity unchanged, so the entries must be
    examined too: without them, a certificate replaced or truncated in a hashed directory, or
    a link retargeted, would keep the old context and a removed authority would still be
    trusted. A certificate file or directory created, replaced, or edited after the first call
    therefore gets a new context on the next call, so fixing `SSL_CERT_FILE` as an error
    message advises takes effect for clients created afterwards without restarting. The proxy
    settings never shape the context, so they are not part of the key. Contexts for settings no
    longer in use stay cached until the process ends.

    Examining the settings costs one directory listing per `SSL_CERT_DIR` directory and one
    `stat` call per entry, on every call; no certificate is read unless a context is built.
    It happens before the module lock is taken. The first call for a key builds the context
    under the lock, reading the bundle from local disk while it is held, so concurrent first
    calls build exactly one; later calls take the lock only to look the context up.

    Args:
        consumer: The library that will use the context. urllib3 and `websockets` never share
            one, because urllib3 writes to the contexts it is given.

    Returns:
        A context that verifies certificates and host names and offers ALPN `http/1.1`:
        `ssl.create_default_context()` when `SSL_CERT_FILE` or `SSL_CERT_DIR` is set to a
        non-empty value (OpenSSL then loads those locations), otherwise a default context that
        trusts only `certifi`'s bundle. Either way it writes TLS secrets to `SSLKEYLOGFILE`
        when that is set, as the standard library does. OpenSSL silently skips an
        `SSL_CERT_FILE` or `SSL_CERT_DIR` it cannot read, so a wrong path shows up as a
        certificate verification failure on the first connection, which the clients report
        naming `SSL_CERT_FILE`.
    """
    cert_file = os.environ.get("SSL_CERT_FILE", "")
    cert_dir = os.environ.get("SSL_CERT_DIR", "")
    key = (
        consumer,
        cert_file,
        _identity(cert_file),
        cert_dir,
        _directories_identity(cert_dir),
        os.environ.get("SSLKEYLOGFILE", ""),
    )
    with _CONTEXTS_LOCK:
        context = _CONTEXTS.get(key)
        if context is None:
            if cert_file or cert_dir:
                context = ssl.create_default_context()
            else:
                context = ssl.create_default_context(cafile=certifi.where())
            context.set_alpn_protocols(_ALPN_PROTOCOLS)
            _CONTEXTS[key] = context
        return context


def _identity(path: str | Path) -> tuple[int, ...]:
    """Return what tells one version of a file or directory from another.

    Args:
        path: The path to examine, following symbolic links; empty when the variable that
            names it is unset.

    Returns:
        The modification time in nanoseconds, the size, and the inode, or `()` when the path is
        empty or names nothing that can be examined (including a symbolic link to a missing
        file).
    """
    if not path:
        return ()
    try:
        status = Path(path).stat()
    except OSError:
        return ()
    return (status.st_mtime_ns, status.st_size, status.st_ino)


def _directories_identity(cert_dir: str) -> tuple[object, ...]:
    """Return what tells one version of the `SSL_CERT_DIR` certificate directories from another.

    OpenSSL looks a certificate up in these directories by the hash of its subject, reading
    the file named `<hash>.<n>` when a connection needs it. Such a file is often a symbolic
    link to a bundle elsewhere, and is often rewritten in place, which changes neither the
    directory's modification time, size, nor inode. So each directory contributes its own
    identity and the name of every entry with the identity of the file it resolves to: what
    OpenSSL reads is that file, so a link changed to point elsewhere shows up as a different
    file. File contents are never read.

    Args:
        cert_dir: The value of `SSL_CERT_DIR`: directories separated by `os.pathsep`, or empty
            when the variable is unset. Empty components are skipped, as OpenSSL skips them.

    Returns:
        One `(directory, directory identity, entries)` triple per directory, in the order
        given, where `entries` holds one `(name, identity)` pair per entry, sorted by name,
        and is empty when the directory cannot be listed. Identities are as `_identity`
        returns them.
    """
    return tuple(
        (directory, _identity(directory), _entries_identity(directory))
        for directory in cert_dir.split(os.pathsep)
        if directory
    )


def _entries_identity(directory: str) -> tuple[tuple[str, tuple[int, ...]], ...]:
    """Return the name and identity of every entry in `directory`, sorted by name.

    Args:
        directory: One directory from `SSL_CERT_DIR`.

    Returns:
        One `(name, identity)` pair per entry, where the identity is that of the file the
        entry resolves to, as `_identity` returns it. Empty when the directory does not exist
        or cannot be listed. An entry removed while it is examined, or a link to a missing
        file, gets the identity `()`.
    """
    root = Path(directory)
    try:
        names = sorted(entry.name for entry in root.iterdir())
    except OSError:
        return ()
    return tuple((name, _identity(root / name)) for name in names)


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
