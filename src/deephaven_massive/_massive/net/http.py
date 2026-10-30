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
`ssl_context`: the latest context built for each consumer, rebuilt under a lock when the
certificate setting changes. This module imports only the standard library and `certifi`.
"""

from __future__ import annotations

import base64
import enum
import logging
import os
import ssl
import threading
import urllib.request
from pathlib import Path
from typing import Final
from urllib.parse import unquote, urlsplit, urlunsplit

import certifi

__all__ = ["TlsConsumer", "ca_file", "https_proxy", "proxy_credentials", "ssl_context"]

_log = logging.getLogger(__name__)


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

_DEFAULT_VERIFY_PATHS: Final = ssl.get_default_verify_paths()
_OPENSSL_CAFILE: Final[str] = _DEFAULT_VERIFY_PATHS.openssl_cafile
"""The certificate bundle OpenSSL was built to load when `SSL_CERT_FILE` is unset."""

_OPENSSL_CAPATH: Final[str] = _DEFAULT_VERIFY_PATHS.openssl_capath
"""The certificate directory OpenSSL was built to search when `SSL_CERT_DIR` is unset."""

_CONTEXTS_LOCK: Final = threading.Lock()
"""Guards `_CONTEXTS`. A leaf lock: no other lock is taken while it is held."""

_CONTEXTS: Final[dict[TlsConsumer, tuple[tuple[object, ...], ssl.SSLContext]]] = {}
"""The latest TLS context built for each consumer, with the certificate setting it was built
for (see `ssl_context`); guarded by `_CONTEXTS_LOCK`."""

_WARNED_LOCK: Final = threading.Lock()
"""Guards `_WARNED`. A leaf lock: no other lock is taken while it is held."""

_WARNED: Final[set[tuple[str, str]]] = set()
"""The `(path, error)` pairs already logged as a certificate directory or entry that cannot be
examined, so each is logged once per process; guarded by `_WARNED_LOCK`."""


def ssl_context(consumer: TlsConsumer) -> ssl.SSLContext:
    """Return the process's client TLS context for `consumer` and the current settings.

    The REST client hands it to urllib3 (for Massive and for an HTTPS proxy) and the
    WebSocket client to `websockets`. Building a context reads and parses a certificate
    bundle, a few milliseconds of CPU, so contexts are shared: the latest one built for each
    consumer is kept, and returned to every later caller while the certificate setting stays
    the same. A configured context is safe to share between threads and connections (CPython
    guards each context's OpenSSL state with its own lock), so callers must not reconfigure
    it; urllib3 writes the same verification mode and ALPN list on its own contexts before
    each connection, which changes nothing.

    The certificate setting is everything that shapes the context. With neither `SSL_CERT_FILE`
    nor `SSL_CERT_DIR` set, that is only `SSLKEYLOGFILE`, since the context trusts the
    `certifi` bundle that ships with the library. With either set, OpenSSL loads two
    locations: the bundle `SSL_CERT_FILE` names, or the one OpenSSL was built with when it is
    unset, and the directories `SSL_CERT_DIR` names (separated by `os.pathsep`), or the one
    OpenSSL was built with when it is unset. The setting is then the three variables, the
    identity of that bundle, and, for each of those directories, its identity and the name and
    identity of every entry in it. An identity is the modification time, change time, size,
    inode, and device of the file a path resolves to (following symbolic links), or nothing
    when the path cannot be examined; the change time moves with a permission change and with
    a rewrite whose modification time was put back. Editing a file in place leaves its
    directory's own identity unchanged, so the entries must be examined too: without them, a
    certificate replaced, truncated, or made unreadable in a hashed directory, or a link
    retargeted, would keep the old context and a removed authority would still be trusted. A
    certificate file or directory created, replaced, or edited after the first call therefore
    gets a new context on the next call, so fixing `SSL_CERT_FILE` as an error message advises
    takes effect for clients created afterwards without restarting. The proxy settings never
    shape the context, so they are not part of the key. A context replaced in the cache stays
    alive only as long as the clients already using it.

    Examining the setting costs, on every call, one `stat` of the bundle, one directory listing
    per certificate directory, and one `stat` per entry: 1 to 1.5 milliseconds for a
    directory of 300 symbolic links. No certificate is read to decide. Building a context
    reads the bundle; OpenSSL reads a directory's hashed files only during a handshake, when a
    certificate needs one, and keeps what it read in that context, which is why a change
    needs a new context. The setting is examined before the module lock is taken. A call whose
    setting differs from the cached one builds the context under the lock, reading the bundle
    from local disk while it is held, so concurrent first calls build exactly one; later calls
    take the lock only to compare the setting. Building a context logs the setting at DEBUG.
    A configured certificate directory that cannot be listed, or an entry of one whose file
    cannot be examined, logs a WARNING once per path and error (OpenSSL's built-in directory
    logs such failures only at DEBUG). All of it is logged after the locks are released.

    Args:
        consumer: The library that will use the context. urllib3 and `websockets` never share
            one, because urllib3 writes to the contexts it is given.

    Returns:
        A context that verifies certificates and host names and offers ALPN `http/1.1`:
        `ssl.create_default_context()` when `SSL_CERT_FILE` or `SSL_CERT_DIR` is set to a
        non-empty value (OpenSSL then loads the locations above), otherwise a default context
        that trusts only `certifi`'s bundle. Either way it writes TLS secrets to
        `SSLKEYLOGFILE` when that is set, as the standard library does. OpenSSL silently skips
        an `SSL_CERT_FILE` or `SSL_CERT_DIR` it cannot read, so a wrong path shows up as a
        certificate verification failure on the first connection, which the clients report
        naming `SSL_CERT_FILE`.
    """
    cert_file = os.environ.get("SSL_CERT_FILE", "")
    cert_dir = os.environ.get("SSL_CERT_DIR", "")
    key_log = os.environ.get("SSLKEYLOGFILE", "")
    uses_openssl = bool(cert_file or cert_dir)
    bundle = (cert_file or _OPENSSL_CAFILE) if uses_openssl else ""
    directories = (
        _directories_identity(cert_dir or _OPENSSL_CAPATH, configured=bool(cert_dir))
        if uses_openssl
        else ()
    )
    key = (cert_file, cert_dir, key_log, _identity(bundle), directories)
    with _CONTEXTS_LOCK:
        cached = _CONTEXTS.get(consumer)
        built = cached is None or cached[0] != key
        if cached is not None and not built:
            context = cached[1]
        else:
            if uses_openssl:
                context = ssl.create_default_context()
            else:
                context = ssl.create_default_context(cafile=certifi.where())
            context.set_alpn_protocols(_ALPN_PROTOCOLS)
            _CONTEXTS[consumer] = (key, context)
    if built:
        _log.debug(
            "built a TLS context for %s trusting %s (SSL_CERT_FILE=%r, SSL_CERT_DIR=%r, "
            "bundle=%r, certificate directories=%r, directory entries: %d, SSLKEYLOGFILE=%r)",
            consumer.value,
            "the OpenSSL locations" if uses_openssl else "certifi's bundle",
            cert_file,
            cert_dir,
            bundle,
            [directory for directory, _, _ in directories],
            sum(len(entries) for _, _, entries in directories),
            key_log,
        )
    return context


def _status_identity(status: os.stat_result) -> tuple[int, ...]:
    """Return the fields of `status` that tell one version of a file from another.

    Returns:
        The modification time and change time in nanoseconds, the size, the inode, and the
        device. The change time moves whenever the file's data or metadata changes, so it
        catches a permission change and a rewrite whose modification time was put back.
    """
    return (status.st_mtime_ns, status.st_ctime_ns, status.st_size, status.st_ino, status.st_dev)


def _identity(path: str) -> tuple[int, ...]:
    """Return what tells one version of a file or directory from another.

    Args:
        path: The path to examine, following symbolic links; empty when there is none.

    Returns:
        The fields `_status_identity` returns, or `()` when the path is empty or names nothing
        that can be examined (including a symbolic link to a missing file).
    """
    if not path:
        return ()
    try:
        status = Path(path).stat()
    except OSError:
        return ()
    return _status_identity(status)


def _directories_identity(
    cert_dir: str, *, configured: bool
) -> tuple[tuple[str, tuple[int, ...], tuple[tuple[str, tuple[int, ...]], ...]], ...]:
    """Return what tells one version of a set of certificate directories from another.

    OpenSSL looks a certificate up in these directories by the hash of its subject, reading
    the file named `<hash>.<n>` when a handshake needs it. Such a file is often a symbolic
    link to a bundle elsewhere, and is often rewritten in place, which leaves the directory's
    own identity unchanged. So each directory contributes its own identity and the name of
    every entry with the identity of the file it resolves to: what OpenSSL reads is that file,
    so a link changed to point elsewhere shows up as a different file. File contents are never
    read.

    Args:
        cert_dir: Directories separated by `os.pathsep`: the value of `SSL_CERT_DIR`, or the
            directory OpenSSL was built with. Empty components are skipped, as OpenSSL skips
            them.
        configured: Whether `cert_dir` comes from `SSL_CERT_DIR`, so that a directory that
            cannot be listed is the user's to fix and logs a WARNING. The directory OpenSSL
            was built with is often absent (the python.org macOS build has none), so failing
            to list it logs only at DEBUG.

    Returns:
        One `(directory, directory identity, entries)` triple per directory, in the order
        given, where `entries` holds one `(name, identity)` pair per entry, sorted by name,
        and is empty when the directory cannot be listed. Identities are as `_identity`
        returns them.
    """
    return tuple(
        (directory, _identity(directory), _entries_identity(directory, configured=configured))
        for directory in cert_dir.split(os.pathsep)
        if directory
    )


def _entries_identity(
    directory: str, *, configured: bool
) -> tuple[tuple[str, tuple[int, ...]], ...]:
    """Return the name and identity of every entry in `directory`, sorted by name.

    Args:
        directory: One certificate directory.
        configured: Whether `directory` comes from `SSL_CERT_DIR`; see
            `_directories_identity`.

    Returns:
        One `(name, identity)` pair per entry, where the identity is that of the file the
        entry resolves to, as `_identity` returns it. Empty when the directory does not exist
        or cannot be listed, which logs the directory and the error: at WARNING, once per
        process for each pair, when `configured`, and otherwise at DEBUG. An entry removed
        while it is examined, or a link to a missing file, gets the identity `()`.
    """
    try:
        with os.scandir(directory) as scan:
            entries = sorted(scan, key=lambda entry: entry.name)
    except OSError as error:
        _report_unexaminable(_UNLISTABLE, directory, error, configured=configured)
        return ()
    return tuple((entry.name, _entry_identity(entry, configured=configured)) for entry in entries)


def _entry_identity(entry: os.DirEntry[str], *, configured: bool) -> tuple[int, ...]:
    """Return the identity of the file a directory entry resolves to, or `()` if there is none.

    A directory listing hands back each entry ready to examine, which costs half as much as
    examining it again by path.

    Args:
        entry: One entry of a certificate directory.
        configured: Whether its directory comes from `SSL_CERT_DIR`; see
            `_directories_identity`.

    Returns:
        The fields `_status_identity` returns, or `()` when the entry was removed, is a
        symbolic link to a missing file, or resolves into a place that cannot be examined.
        Every such failure is logged with the entry's path and the error: at WARNING, once
        per process for each pair, when `configured`, and otherwise at DEBUG.
    """
    try:
        return _status_identity(entry.stat())
    except OSError as error:
        _report_unexaminable(_UNEXAMINABLE, entry.path, error, configured=configured)
        return ()


_UNLISTABLE: Final = (
    "cannot list the certificate directory %r (%s): OpenSSL may still find certificates there "
    "by their hashed names, but a change to them goes unnoticed, so clients created afterwards "
    "may keep trusting a certificate that was removed"
)
"""The message for a certificate directory that cannot be listed."""

_UNEXAMINABLE: Final = (
    "cannot examine the certificate directory entry %r (%s): OpenSSL cannot load it either, "
    "and the library notices when it can be examined again"
)
"""The message for an entry of a certificate directory whose file cannot be examined."""


def _report_unexaminable(message: str, path: str, error: OSError, *, configured: bool) -> None:
    """Log that a certificate directory or one of its entries cannot be examined.

    A location the user configured logs a WARNING, once per process for each path and error,
    since the setting is examined every time a client is created. OpenSSL's built-in
    directory is often absent or partly dangling (the python.org macOS build has none), so
    its failures log only at DEBUG, every time.

    Args:
        message: `_UNLISTABLE` or `_UNEXAMINABLE`, formatted with the path and the error.
        path: The directory or entry that could not be examined.
        error: Why, as the operating system reported it.
        configured: Whether the location comes from `SSL_CERT_DIR`.
    """
    if not configured:
        _log.debug(message, path, error)
        return
    warning = (path, str(error))
    with _WARNED_LOCK:
        first = warning not in _WARNED
        _WARNED.add(warning)
    if first:
        _log.warning(message, path, error)


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
