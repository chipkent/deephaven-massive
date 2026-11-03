"""Download Massive's published specifications and store them as the canonical snapshot.

`python -m deephaven_massive._massive.refresh --fetch` calls `fetch`, which replaces the
vendored snapshot (`vendor/massive/` in the source checkout, whose layout and manifest
`snapshot` owns) with the live vendor sources:

- `openapi.json`: the REST specification from `https://api.massive.com/openapi`. The older
  `https://api.massive.com/specs/rest.json` is a stale subset of about 33 paths and is never
  used;
- `websocket.json`: the WebSocket specification from
  `https://api.massive.com/specs/websocket.json`;
- `llms.txt`: the documentation index from `https://massive.com/docs/llms.txt`;
- `rest/<area>/.../<page>.md`: every REST documentation page the index links, which is how the
  refresh learns which OpenAPI paths are documented (see `docs`);
- `websocket/<class>/<page>.md`: every WebSocket documentation page the index links, which
  names the cluster and channel of its feed;
- `flat-files/<class>/.../<page>.md`: every flat-file documentation page the index links: the
  overviews, whose tables name each dataset's bucket prefix, and the dataset pages, whose
  "Columns" tables are the flat-file schemas;
- `knowledge-base/<slug>.md`: every knowledge-base article the library relies on
  (`knowledge_base.ARTICLES`), downloaded as its HTML page from
  `https://massive.com/knowledge-base/article/<slug>` and stored as the Markdown
  `knowledge_base.article_markdown` extracts from it, since articles are published as HTML
  only;
- `manifest.json`: the fetch date and, for each file above, its URL, the SHA-256 of the bytes
  as fetched, and the SHA-256 of the file as stored.

**Canonical storage.** Every file is rewritten into one canonical form, so a refresh diff
reads line by line and the repository's whitespace hooks never need an exception: JSON is
re-serialized with an indent of one space and a final newline, keeping the vendor's key order
(the order of an object's properties is the documented column order, so it is never sorted);
text has the trailing whitespace of every line removed, trailing blank lines dropped, and one
final newline. The stored hash covers that canonical form, the fetched hash the raw bytes
(for an article, the HTML page).

**Untrusted input.** The downloads are data, never code: JSON is parsed with the standard
library, text is decoded as UTF-8, nothing is evaluated, and a page URL is accepted only below
its area's prefix (`https://massive.com/docs/rest/`, `.../websocket/`, or `.../flat-files/`)
with plain lower-case path segments (`docs.page_file`), so no URL can name a file outside the
snapshot. Every download completes, and every file is
canonicalized, before anything is written; `snapshot.store` then builds the new snapshot in
a sibling directory and swaps it in through a backup directory, so a failed fetch leaves the
old snapshot untouched.

The network is reached only through the `get` callable that `fetch` receives. The default,
`HttpGetter`, uses urllib3 with the CA bundle and proxy of `net.http` (sending a user and
password in the proxy URL as `Proxy-Authorization`, as the REST client does), sends the
library's `User-Agent`, and follows no redirects. It retries only HTTP 429, which the
documentation host answers to a burst of page requests (see `HttpGetter`); any other failure
stops the fetch, and a person reruns the command. Tests pass a dictionary-backed `get` instead.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import time
import urllib.parse
from typing import TYPE_CHECKING

import urllib3

from deephaven_massive._massive.net import USER_AGENT, http
from deephaven_massive._massive.net.errors import is_proxy_auth_failure
from deephaven_massive._massive.net.retry import retry_after_seconds
from deephaven_massive._massive.refresh import docs, knowledge_base, snapshot

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path
    from typing import Self

__all__ = [
    "LLMS_URL",
    "OPENAPI_URL",
    "WEBSOCKET_URL",
    "FetchError",
    "HttpGetter",
    "canonical_json",
    "canonical_text",
    "fetch",
]

OPENAPI_URL = "https://api.massive.com/openapi"
"""Where Massive publishes its full REST specification."""

WEBSOCKET_URL = "https://api.massive.com/specs/websocket.json"
"""Where Massive publishes its WebSocket specification."""

LLMS_URL = "https://massive.com/docs/llms.txt"
"""Where Massive publishes its documentation index."""

_log = logging.getLogger(__name__)

# Connect and read timeouts for one download, in seconds. The specification is about 1 MB.
_TIMEOUT = urllib3.Timeout(connect=10.0, read=60.0)
_HTTP_OK = 200
_HTTP_TOO_MANY_REQUESTS = 429
# The waits before each retry of a 429 without a usable Retry-After, in seconds; a 429 after
# the last wait fails.
_RATE_LIMIT_WAITS = (2.0, 4.0, 8.0, 16.0, 32.0)
# The longest wait obeyed, in seconds, whatever Retry-After asks.
_MAX_WAIT = 60.0


class FetchError(RuntimeError):
    """A download failed or returned something that is not the expected document.

    The snapshot on disk is unchanged when this is raised.
    """


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


class HttpGetter:
    """Download public documents over HTTPS with the wire package's TLS and proxy settings.

    One urllib3 pool is kept per host, so the hundred-odd documentation pages reuse their
    connections until `close()` (or the end of a `with` block) closes them. The proxy for a
    host is decided once, from `net.http.https_proxy`, when the host is first requested; a
    user and password in the proxy URL go to the proxy as `Proxy-Authorization`
    (`net.http.proxy_credentials`), and the pool holds the URL without them, so no error
    quotes the password. Instances are used by one thread, the refresh command's.

    The documentation host answers a burst of page requests with HTTP 429. A 429 is the one
    answer that is retried: the getter waits what the `Retry-After` header asks, in either of
    its forms (seconds or an HTTP date, read by `net.retry.retry_after_seconds`, which logs an
    unusable value at WARNING) and never more than 60 seconds, else 2, 4, 8, 16, then 32
    seconds, and gives up after five waits. Every other failure fails at once; a person reruns
    the command.

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
            `PoolManager`, both verifying certificates with a `net.http.new_ssl_context()` of
            their own, the proxy's own certificate included when it is an HTTPS proxy.
        """
        if host not in self._pools:
            proxy = http.https_proxy(host)
            context = http.new_ssl_context()  # urllib3 writes to it: never shared
            if proxy:
                proxy_url, proxy_headers = http.proxy_credentials(proxy)
                self._pools[host] = urllib3.ProxyManager(
                    proxy_url,
                    proxy_headers=proxy_headers,
                    proxy_ssl_context=context,
                    ssl_context=context,
                )
                _log.debug(
                    "downloads from %s go through the proxy %s (credentials sent: %s)",
                    host,
                    proxy_url,
                    bool(proxy_headers),
                )
            else:
                self._pools[host] = urllib3.PoolManager(ssl_context=context)
                _log.debug("downloads from %s connect directly, through no proxy", host)
        return self._pools[host]

    def __call__(self, url: str) -> bytes:
        """Download one document.

        Args:
            url: An absolute URL.

        Returns:
            The response body, exactly as received (urllib3 removes any gzip encoding).

        Raises:
            FetchError: If the connection fails, the proxy refuses its credentials (HTTP 407 to
                the tunnel request, which names `HTTPS_PROXY` and never the password), the
                server still answers 429 after five waits, or it answers anything else but
                HTTP 200 (a redirect included, since none is followed).
        """
        host = urllib.parse.urlsplit(url).hostname or ""
        pool = self._pool(host)
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
                if is_proxy_auth_failure(error):
                    raise FetchError(
                        f"GET {url}: the proxy refused its credentials (HTTP 407 for a tunnel "
                        f"to {host}); check the user and password in HTTPS_PROXY"
                    ) from error
                raise FetchError(f"GET {url} failed: {error}") from error
            if response.status == _HTTP_OK:
                return response.data
            wait = next(waits, None)
            if response.status != _HTTP_TOO_MANY_REQUESTS or wait is None:
                raise FetchError(f"GET {url} answered HTTP {response.status}")
            asked = retry_after_seconds(response.headers.get("Retry-After"), time.time())
            waited = min(wait if asked is None else asked, _MAX_WAIT)
            _log.debug(
                "GET %s answered HTTP 429; Retry-After asks %s s, the cap is %s s; waiting %s s",
                url,
                asked,
                _MAX_WAIT,
                waited,
            )
            self._sleep(waited)


def _download(get: Callable[[str], bytes]) -> list[snapshot.VendorFile]:
    """Download every vendor source and put each in canonical form.

    Args:
        get: Returns the body of a URL, raising on failure.

    A `FetchError` from `get` or from canonicalizing a document propagates.

    Returns:
        The two specifications, the index, the pages of each documentation area (REST,
        WebSocket, flat files), each area in index order, then the knowledge-base articles in
        `knowledge_base.ARTICLES` order.

    Raises:
        FetchError: If the index links no page of an area, or a page outside its area's
            documentation, or an article's page has no title or no article text.
    """
    files = []
    for name, url in ((snapshot.OPENAPI, OPENAPI_URL), (snapshot.WEBSOCKET, WEBSOCKET_URL)):
        raw = get(url)
        files.append(
            snapshot.VendorFile(name=name, url=url, raw=raw, stored=canonical_json(raw, url))
        )
    index_raw = get(LLMS_URL)
    index = canonical_text(index_raw, LLMS_URL)
    files.append(snapshot.VendorFile(name=snapshot.LLMS, url=LLMS_URL, raw=index_raw, stored=index))
    for area in snapshot.AREA_DIRECTORIES:
        try:
            urls = docs.page_urls(index.decode(), area)
        except docs.DocsError as error:
            raise FetchError(f"{LLMS_URL}: {error}") from None
        for url in urls:
            page = get(url)
            name = snapshot.page_file(url, area)
            files.append(
                snapshot.VendorFile(name=name, url=url, raw=page, stored=canonical_text(page, url))
            )
    for slug in knowledge_base.ARTICLES:
        url = knowledge_base.article_url(slug)
        page = get(url)
        try:
            text = knowledge_base.article_markdown(_decode(page, url), url)
        except knowledge_base.KnowledgeBaseError as error:
            raise FetchError(str(error)) from None
        files.append(
            snapshot.VendorFile(
                name=snapshot.article_file(slug),
                url=url,
                raw=page,
                stored=canonical_text(text.encode(), url),
            )
        )
    return files


def fetch(
    directory: Path,
    *,
    get: Callable[[str], bytes] | None = None,
    today: dt.date | None = None,
) -> list[str]:
    """Replace the snapshot in `directory` with freshly downloaded vendor sources.

    Everything is downloaded and canonicalized first; only then does `snapshot.store` write
    the new snapshot with its manifest and swap it into place, so a failure while downloading
    leaves the old snapshot as it was. Files of the old snapshot that the new one lacks (a
    page Massive removed) are gone afterwards.

    Args:
        directory: The snapshot directory, normally `snapshot.DIRECTORY`.
        get: Returns the body of a URL and raises `FetchError` on failure; an `HttpGetter`
            when omitted. Network access happens only through it.
        today: The fetch date written to the manifest; today's UTC date when omitted.

    A `FetchError` (a download failed, a specification is not JSON, a document is not UTF-8,
    the index links no page of an area or a page outside its area, or an article's page has
    no title or no article text) propagates and leaves the snapshot unchanged.

    Returns:
        The names of the stored files, relative to `directory`, manifest last.
    """
    if get is None:
        with HttpGetter() as getter:
            files = _download(getter)
    else:
        files = _download(get)
    return snapshot.store(directory, files, fetched=today or dt.datetime.now(dt.UTC).date())
