"""Download Massive's published specifications and store them as the canonical snapshot.

`python -m deephaven_massive._massive.refresh --fetch` calls `fetch`, which replaces the
snapshot directory (`refresh/snapshot/`) with the live vendor sources:

- `openapi.json`: the REST specification from `https://api.massive.com/openapi`. The older
  `https://api.massive.com/specs/rest.json` is a stale subset of about 33 paths and is never
  used;
- `llms.txt`: the documentation index from `https://massive.com/docs/llms.txt`;
- `rest/<area>/.../<page>.md`: every REST documentation page the index links, which is how the
  refresh learns which OpenAPI paths are documented (see `docs`);
- `manifest.json`: the fetch date and, for each file above, its URL, the SHA-256 of the bytes
  as fetched, and the SHA-256 of the file as stored.

**Canonical storage.** Every file is rewritten into one canonical form, so a refresh diff
reads line by line and the repository's whitespace hooks never need an exception: JSON is
re-serialized with an indent of one space and a final newline, keeping the vendor's key order
(the order of an object's properties is the documented column order, so it is never sorted);
text has the trailing whitespace of every line removed, trailing blank lines dropped, and one
final newline. The stored hash covers that canonical form, the fetched hash the raw bytes.

**Untrusted input.** The downloads are data, never code: JSON is parsed with the standard
library, text is decoded as UTF-8, nothing is evaluated, and a page URL is accepted only below
`https://massive.com/docs/rest/` with plain lower-case path segments (`docs.page_file`), so no
URL can name a file outside the snapshot. Every download completes, and every file is
canonicalized, before anything is written; the new snapshot is then built in a sibling
directory and swapped in through a backup directory, so a failed fetch leaves the old snapshot
untouched.

The network is reached only through the `get` callable that `fetch` receives. The default,
`HttpGetter`, uses urllib3 with the CA bundle and proxy of `net.http`, sends the library's
`User-Agent`, and follows no redirects. It retries only HTTP 429, which the documentation host
answers to a burst of page requests (see `HttpGetter`); any other failure stops the fetch, and
a person reruns the command. Tests pass a dictionary-backed `get` instead.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import shutil
import time
import urllib.parse
from dataclasses import dataclass
from typing import TYPE_CHECKING

import urllib3

from deephaven_massive._massive.net import USER_AGENT, http
from deephaven_massive._massive.refresh import docs

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from pathlib import Path
    from typing import Self

__all__ = [
    "LLMS_URL",
    "MANIFEST",
    "OPENAPI_URL",
    "FetchError",
    "HttpGetter",
    "canonical_json",
    "canonical_text",
    "fetch",
]

OPENAPI_URL = "https://api.massive.com/openapi"
"""Where Massive publishes its full REST specification."""

LLMS_URL = "https://massive.com/docs/llms.txt"
"""Where Massive publishes its documentation index."""

OPENAPI = "openapi.json"
"""The snapshot file of the REST specification."""

LLMS = "llms.txt"
"""The snapshot file of the documentation index."""

REST_PAGES = "rest"
"""The snapshot directory of the REST documentation pages."""

MANIFEST = "manifest.json"
"""The snapshot file that records where every other file came from."""

# Connect and read timeouts for one download, in seconds. The specification is about 1 MB.
_TIMEOUT = urllib3.Timeout(connect=10.0, read=60.0)
_HTTP_OK = 200
_HTTP_TOO_MANY_REQUESTS = 429
# The waits before each retry of a 429 without a numeric Retry-After, in seconds; a 429 after
# the last wait fails.
_RATE_LIMIT_WAITS = (2.0, 4.0, 8.0, 16.0, 32.0)
# The longest wait obeyed, in seconds, whatever Retry-After asks.
_MAX_WAIT = 60.0


class FetchError(RuntimeError):
    """A download failed or returned something that is not the expected document.

    The snapshot on disk is unchanged when this is raised.
    """


def _sha256(data: bytes) -> str:
    """Return the hexadecimal SHA-256 digest of `data`.

    Args:
        data: The bytes to hash.

    Returns:
        64 lower-case hexadecimal digits.
    """
    return hashlib.sha256(data).hexdigest()


def _decode(raw: bytes, url: str) -> str:
    """Decode a download as UTF-8 text.

    Args:
        raw: The downloaded bytes.
        url: Where they came from, for the error message.

    Returns:
        The text.

    Raises:
        FetchError: If the bytes are not UTF-8.
    """
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError as error:
        raise FetchError(f"{url} is not UTF-8 text: {error}") from None


def canonical_json(raw: bytes, url: str) -> bytes:
    """Return a JSON document in the snapshot's canonical form.

    Args:
        raw: The document as downloaded.
        url: Where it came from, for the error message.

    Returns:
        UTF-8 JSON with the document's own key order, a one-space indent, non-ASCII characters
        kept as they are, and a final newline.

    Raises:
        FetchError: If the bytes are not UTF-8 or not JSON.
    """
    try:
        document = json.loads(_decode(raw, url))
    except json.JSONDecodeError as error:
        raise FetchError(f"{url} is not JSON: {error}") from None
    return (json.dumps(document, indent=1, ensure_ascii=False) + "\n").encode()


def canonical_text(raw: bytes, url: str) -> bytes:
    """Return a text document in the snapshot's canonical form.

    Bytes that are not UTF-8 raise `FetchError`, naming `url`.

    Args:
        raw: The document as downloaded.
        url: Where it came from, for the error message.

    Returns:
        UTF-8 text with every line's trailing whitespace removed (carriage returns included),
        trailing blank lines dropped, and exactly one final newline.
    """
    lines = [line.rstrip() for line in _decode(raw, url).splitlines()]
    return ("\n".join(lines).rstrip("\n") + "\n").encode()


@dataclass(frozen=True, kw_only=True)
class _Stored:
    """One snapshot file, ready to write.

    Attributes:
        name: The path relative to the snapshot directory, with `/` separators.
        url: Where it was downloaded from.
        raw: The bytes as downloaded.
        stored: The canonical bytes to write.
    """

    name: str
    url: str
    raw: bytes
    stored: bytes


class HttpGetter:
    """Download public documents over HTTPS with the wire package's TLS and proxy settings.

    One urllib3 pool is kept per host, so the hundred-odd documentation pages reuse their
    connections until `close()` (or the end of a `with` block) closes them. The proxy for a
    host is decided once, from `net.http.https_proxy`, when the host is first requested.
    Instances are used by one thread, the refresh command's.

    The documentation host answers a burst of page requests with HTTP 429. A 429 is the one
    answer that is retried: the getter waits the `Retry-After` seconds when the server sends
    a number (never more than 60), else 2, 4, 8, 16, then 32 seconds, and gives up after five
    waits. Every other
    failure fails at once; a person reruns the command.

    Example:
        ::

            from deephaven_massive._massive.refresh.fetch import LLMS_URL, HttpGetter

            with HttpGetter() as get:
                index = get(LLMS_URL)
    """

    def __init__(self, *, sleep: Callable[[float], None] = time.sleep) -> None:
        """Create a getter with no open connections.

        Args:
            sleep: Waits the given seconds before retrying a 429; `time.sleep` unless a test
                passes its own.
        """
        self._pools: dict[str, urllib3.PoolManager] = {}
        self._sleep = sleep

    def __enter__(self) -> Self:
        """Return the getter, which `__exit__` closes.

        Returns:
            This getter.
        """
        return self

    def __exit__(self, *_: object) -> None:
        """Close every pooled connection."""
        self.close()

    def close(self) -> None:
        """Close every pooled connection; a later download opens new ones."""
        for pool in self._pools.values():
            pool.clear()
        self._pools.clear()

    def _pool(self, host: str) -> urllib3.PoolManager:
        """Return the connection pool for `host`, creating it on first use.

        Args:
            host: The host name, without scheme or port.

        Returns:
            A `ProxyManager` when `net.http.https_proxy(host)` names a proxy, else a plain
            `PoolManager`, both verifying certificates with `net.http.ssl_context()`.
        """
        if host not in self._pools:
            proxy = http.https_proxy(host)
            context = http.ssl_context()
            self._pools[host] = (
                urllib3.ProxyManager(proxy, ssl_context=context)
                if proxy
                else urllib3.PoolManager(ssl_context=context)
            )
        return self._pools[host]

    def __call__(self, url: str) -> bytes:
        """Download one document.

        Args:
            url: An absolute URL.

        Returns:
            The response body, exactly as received (urllib3 removes any gzip encoding).

        Raises:
            FetchError: If the connection fails, the server still answers 429 after five
                waits, or it answers anything else but HTTP 200 (a redirect included, since
                none is followed).
        """
        pool = self._pool(urllib.parse.urlsplit(url).hostname or "")
        waits = iter(_RATE_LIMIT_WAITS)
        while True:
            try:
                response = pool.request(
                    "GET",
                    url,
                    headers={"User-Agent": USER_AGENT},
                    timeout=_TIMEOUT,
                    retries=False,
                    redirect=False,
                )
            except urllib3.exceptions.HTTPError as error:
                raise FetchError(f"GET {url} failed: {error}") from error
            if response.status == _HTTP_OK:
                return response.data
            wait = next(waits, None)
            if response.status != _HTTP_TOO_MANY_REQUESTS or wait is None:
                raise FetchError(f"GET {url} answered HTTP {response.status}")
            retry_after = response.headers.get("Retry-After", "")
            asked = float(retry_after) if retry_after.isdigit() else wait
            self._sleep(min(asked, _MAX_WAIT))


def _download(get: Callable[[str], bytes]) -> list[_Stored]:
    """Download every vendor source and put each in canonical form.

    Args:
        get: Returns the body of a URL, raising on failure.

    A `FetchError` from `get` or from canonicalizing a document propagates.

    Returns:
        The specification, the index, then the REST pages in index order.

    Raises:
        FetchError: If the index links a page outside the REST documentation.
    """
    spec = get(OPENAPI_URL)
    files = [
        _Stored(name=OPENAPI, url=OPENAPI_URL, raw=spec, stored=canonical_json(spec, OPENAPI_URL))
    ]
    index_raw = get(LLMS_URL)
    index = canonical_text(index_raw, LLMS_URL)
    files.append(_Stored(name=LLMS, url=LLMS_URL, raw=index_raw, stored=index))
    try:
        urls = docs.rest_page_urls(index.decode())
    except docs.DocsError as error:
        raise FetchError(f"{LLMS_URL}: {error}") from None
    for url in urls:
        page = get(url)
        name = f"{REST_PAGES}/{docs.page_file(url)}"
        files.append(_Stored(name=name, url=url, raw=page, stored=canonical_text(page, url)))
    return files


def _manifest(files: list[_Stored], fetched: dt.date) -> bytes:
    """Return the manifest of a snapshot, in canonical JSON.

    Args:
        files: The snapshot's files.
        fetched: The day of the fetch.

    Returns:
        `{"fetched": "<YYYY-MM-DD>", "files": {<name>: {"url", "fetched_sha256",
        "stored_sha256"}}}`, canonical.
    """
    document: Mapping[str, object] = {
        "fetched": fetched.isoformat(),
        "files": {
            file.name: {
                "url": file.url,
                "fetched_sha256": _sha256(file.raw),
                "stored_sha256": _sha256(file.stored),
            }
            for file in files
        },
    }
    return (json.dumps(document, sort_keys=True, indent=1) + "\n").encode()


def fetch(
    directory: Path,
    *,
    get: Callable[[str], bytes] | None = None,
    today: dt.date | None = None,
) -> list[str]:
    """Replace the snapshot in `directory` with freshly downloaded vendor sources.

    Everything is downloaded and canonicalized first. The new snapshot is then written to a
    sibling directory (`<name>-incoming`); the old one is renamed aside (`<name>-previous`),
    the new one is renamed into place, and only then is the old one removed. A failure while
    downloading leaves the old snapshot as it was, and at no point is the snapshot directory
    half written. Files of the old snapshot that the new one lacks (a page Massive removed)
    are gone afterwards.

    Args:
        directory: The snapshot directory, normally `refresh/snapshot/`. Its parent must exist.
        get: Returns the body of a URL and raises `FetchError` on failure; an `HttpGetter`
            when omitted. Network access happens only through it.
        today: The fetch date written to the manifest; today's UTC date when omitted.

    A `FetchError` (a download failed, the specification is not JSON, a document is not
    UTF-8, or the index links a page outside the REST documentation) propagates and leaves
    the snapshot unchanged.

    Returns:
        The names of the stored files, relative to `directory`, manifest last.
    """
    if get is None:
        with HttpGetter() as getter:
            files = _download(getter)
    else:
        files = _download(get)
    manifest = _manifest(files, today or dt.datetime.now(dt.UTC).date())
    staging = directory.with_name(f"{directory.name}-incoming")
    backup = directory.with_name(f"{directory.name}-previous")
    shutil.rmtree(staging, ignore_errors=True)
    shutil.rmtree(backup, ignore_errors=True)
    for file in files:
        target = staging / file.name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(file.stored)
    (staging / MANIFEST).write_bytes(manifest)
    if directory.exists():
        directory.rename(backup)
    staging.rename(directory)
    shutil.rmtree(backup, ignore_errors=True)
    return [file.name for file in files] + [MANIFEST]
