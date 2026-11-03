from __future__ import annotations

import base64
import datetime as dt
import hashlib
import json
import logging
import ssl
import threading
from http.server import BaseHTTPRequestHandler
from typing import TYPE_CHECKING, Self

import pytest
import urllib3

from deephaven_massive._massive.net import USER_AGENT, http
from deephaven_massive._massive.refresh import fetch, knowledge_base
from tests._fakes.rest import REFUSED_PORT, LoopbackHTTPServer

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from pathlib import Path

# --- Canonical forms ------------------------------------------------------------------------


def test_canonical_json_keeps_the_vendor_order_indents_and_ends_with_a_newline() -> None:
    # The order of an object's properties is the documented column order; it is never sorted.
    raw = '{"b": [1, {"d": "é", "c": null}], "a": true}'.encode()
    expected = '{\n "b": [\n  1,\n  {\n   "d": "é",\n   "c": null\n  }\n ],\n "a": true\n}\n'
    assert fetch.canonical_json(raw, "u") == expected.encode()


def test_canonical_json_rejects_what_is_not_json() -> None:
    with pytest.raises(fetch.FetchError, match="u is not JSON"):
        fetch.canonical_json(b"<html>", "u")


def test_canonical_forms_reject_what_is_not_utf8() -> None:
    with pytest.raises(fetch.FetchError, match="u is not UTF-8"):
        fetch.canonical_json(b"\xff", "u")
    with pytest.raises(fetch.FetchError, match="u is not UTF-8"):
        fetch.canonical_text(b"\xff", "u")


def test_canonical_text_strips_trailing_whitespace_and_blank_lines() -> None:
    raw = b"# Title  \r\n\r\n| a | b |\t\r\nlast line\n\n\n"
    assert fetch.canonical_text(raw, "u") == b"# Title\n\n| a | b |\nlast line\n"


def test_canonical_text_adds_a_missing_final_newline() -> None:
    assert fetch.canonical_text(b"one", "u") == b"one\n"


# --- fetch with an injected getter ----------------------------------------------------------

_PAGE = "https://massive.com/docs/rest/stocks/trades-quotes/trades.md"
_FEED = "https://massive.com/docs/websocket/stocks/trades.md"
_FILE = "https://massive.com/docs/flat-files/stocks/trades.md"
_INDEX = (
    f"# Massive\n\n## REST API\n\n- [Trades]({_PAGE}): Trades   \n"
    f"## WebSocket\n- [Trades]({_FEED}): Trades\n"
    f"## Flat Files\n- [Trades]({_FILE}): Trades\n"
)
_SPEC = b'{"paths": {}, "openapi": "3.0.3"}'
_SLUG = next(iter(knowledge_base.ARTICLES))
_ARTICLE = knowledge_base.article_url(_SLUG)
_ARTICLE_PAGE = (
    b"<html><head><title>A question? | Massive</title></head><body><nav>Menu</nav>"
    b"<article><p>Five   a minute.  </p></article></body></html>"
)
_WS_SPEC = b'{"paths": {"/stocks/T": {}}}'


def _sources(**overrides: bytes) -> dict[str, bytes]:
    sources = {
        fetch.OPENAPI_URL: _SPEC,
        fetch.WEBSOCKET_URL: _WS_SPEC,
        fetch.LLMS_URL: _INDEX.encode(),
        _PAGE: b"**Endpoint:** `GET /v3/trades/{stockTicker}`  \n",
        _FEED: b"The channel prefix for this feed is `T`.\n",
        _FILE: b"## Columns\n",
        _ARTICLE: _ARTICLE_PAGE,
    }
    sources.update(overrides)
    return sources


def _getter(
    sources: dict[str, bytes], requested: list[str] | None = None
) -> Callable[[str], bytes]:
    def get(url: str) -> bytes:
        if requested is not None:
            requested.append(url)
        if url not in sources:
            raise fetch.FetchError(f"GET {url} answered HTTP 404")
        return sources[url]

    return get


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def test_fetch_stores_every_source_canonically_with_a_manifest(tmp_path: Path) -> None:
    requested: list[str] = []
    target = tmp_path / "snapshot"
    names = fetch.fetch(target, get=_getter(_sources(), requested), today=dt.date(2026, 10, 26))
    assert requested == [
        fetch.OPENAPI_URL,
        fetch.WEBSOCKET_URL,
        fetch.LLMS_URL,
        _PAGE,
        _FEED,
        _FILE,
        _ARTICLE,
    ]
    assert names == [
        "openapi.json",
        "websocket.json",
        "llms.txt",
        "rest/stocks/trades-quotes/trades.md",
        "websocket/stocks/trades.md",
        "flat-files/stocks/trades.md",
        f"knowledge-base/{_SLUG}.md",
        "manifest.json",
    ]
    stored_spec = (target / "openapi.json").read_bytes()
    assert stored_spec == b'{\n "paths": {},\n "openapi": "3.0.3"\n}\n'
    assert (target / "websocket.json").read_bytes() == b'{\n "paths": {\n  "/stocks/T": {}\n }\n}\n'
    assert (target / "flat-files/stocks/trades.md").read_bytes() == b"## Columns\n"
    page = (target / "rest/stocks/trades-quotes/trades.md").read_bytes()
    assert page == b"**Endpoint:** `GET /v3/trades/{stockTicker}`\n"
    manifest = json.loads((target / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["fetched"] == "2026-10-26"
    assert manifest["files"]["openapi.json"] == {
        "url": fetch.OPENAPI_URL,
        "fetched_sha256": _sha(_SPEC),
        "stored_sha256": _sha(stored_spec),
    }
    assert manifest["files"]["rest/stocks/trades-quotes/trades.md"]["stored_sha256"] == _sha(page)
    # An article is stored as the Markdown of its page; the manifest hashes the page as fetched.
    article = (target / "knowledge-base" / f"{_SLUG}.md").read_bytes()
    assert article == b"# A question?\n\nFive a minute.\n"
    assert manifest["files"][f"knowledge-base/{_SLUG}.md"] == {
        "url": _ARTICLE,
        "fetched_sha256": _sha(_ARTICLE_PAGE),
        "stored_sha256": _sha(article),
    }
    assert set(manifest["files"]) == set(names[:-1])
    assert (target / "manifest.json").read_text(encoding="utf-8").endswith("}\n")


def test_fetch_replaces_the_old_snapshot_entirely(tmp_path: Path) -> None:
    # The swap itself is tested with `snapshot.store`; here only that fetch goes through it.
    target = tmp_path / "massive"
    (target / "rest" / "gone").mkdir(parents=True)
    (target / "rest" / "gone" / "page.md").write_text("removed by Massive\n", encoding="utf-8")
    fetch.fetch(target, get=_getter(_sources()), today=dt.date(2026, 1, 1))
    assert not (target / "rest" / "gone").exists()
    assert (target / "manifest.json").is_file()


def test_a_failed_fetch_leaves_the_old_snapshot_untouched(tmp_path: Path) -> None:
    target = tmp_path / "snapshot"
    target.mkdir()
    (target / "openapi.json").write_text("old\n", encoding="utf-8")
    sources = _sources()
    del sources[_PAGE]
    with pytest.raises(fetch.FetchError, match="HTTP 404"):
        fetch.fetch(target, get=_getter(sources))
    assert sorted(p.name for p in target.iterdir()) == ["openapi.json"]
    assert (target / "openapi.json").read_text(encoding="utf-8") == "old\n"


def test_an_index_linking_outside_an_area_s_documentation_fails(tmp_path: Path) -> None:
    index = _INDEX.encode() + b"- [Evil](https://example.com/x.md)\n"
    with pytest.raises(fetch.FetchError, match="not a Flat Files documentation page"):
        fetch.fetch(tmp_path / "s", get=_getter(_sources(**{fetch.LLMS_URL: index})))


def test_an_index_without_an_area_s_links_fails(tmp_path: Path) -> None:
    index = _INDEX.split("## WebSocket", maxsplit=1)[0].encode()
    with pytest.raises(fetch.FetchError, match="no 'WebSocket' links"):
        fetch.fetch(tmp_path / "s", get=_getter(_sources(**{fetch.LLMS_URL: index})))


@pytest.mark.parametrize(
    ("page", "message"),
    [
        (b"<html><title>A question?</title><main>No article.</main></html>", "no <article>"),
        (b"\xff", "is not UTF-8"),
    ],
    ids=["no-article", "not-utf8"],
)
def test_an_article_page_without_its_article_fails_and_keeps_the_old_snapshot(
    tmp_path: Path, page: bytes, message: str
) -> None:
    target = tmp_path / "snapshot"
    target.mkdir()
    (target / "openapi.json").write_text("old\n", encoding="utf-8")
    with pytest.raises(fetch.FetchError, match=f"^{_ARTICLE} .*{message}"):
        fetch.fetch(target, get=_getter(_sources(**{_ARTICLE: page})))
    assert sorted(p.name for p in target.iterdir()) == ["openapi.json"]


def test_the_fetch_date_defaults_to_today_in_utc(tmp_path: Path) -> None:
    fetch.fetch(tmp_path / "s", get=_getter(_sources()))
    manifest = json.loads((tmp_path / "s" / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["fetched"] in {
        (dt.datetime.now(dt.UTC) - dt.timedelta(minutes=1)).date().isoformat(),
        dt.datetime.now(dt.UTC).date().isoformat(),
    }


# --- HttpGetter against a local server -------------------------------------------------------


class _Server:
    """A local HTTP server that answers from a script and records each request."""

    def __init__(self) -> None:
        self.script: list[tuple[int, dict[str, str], bytes]] = []
        self.requests: list[tuple[str, str]] = []
        self.proxy_authorizations: list[str | None] = []
        server = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                server.requests.append((self.path, self.headers.get("User-Agent", "")))
                server.proxy_authorizations.append(self.headers.get("Proxy-Authorization"))
                status, headers, body = server.script.pop(0)
                self.send_response(status)
                for name, value in headers.items():
                    self.send_header(name, value)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_CONNECT(self) -> None:
                # A proxy that refuses the tunnel's credentials, as a corporate proxy does.
                server.requests.append((f"CONNECT {self.path}", self.headers.get("User-Agent", "")))
                server.proxy_authorizations.append(self.headers.get("Proxy-Authorization"))
                self.send_response(407, "Proxy Authentication Required")
                self.send_header("Content-Length", "0")
                self.end_headers()

        self.http = LoopbackHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(
            target=self.http.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True
        )
        self.thread.start()

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.http.server_address[1]}"

    def close(self) -> None:
        self.http.shutdown()
        self.http.server_close()
        self.thread.join()


@pytest.fixture
def server(monkeypatch: pytest.MonkeyPatch) -> Iterator[_Server]:
    for name in ("HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy", "NO_PROXY", "no_proxy"):
        monkeypatch.delenv(name, raising=False)
    running = _Server()
    yield running
    running.close()


def test_a_200_returns_the_body_and_names_the_library(server: _Server) -> None:
    server.script = [(200, {}, b"body")]
    with fetch.HttpGetter() as getter:
        assert getter(f"{server.url}/docs/llms.txt") == b"body"
    assert server.requests == [("/docs/llms.txt", USER_AGENT)]


@pytest.mark.parametrize("status", [404, 500, 301])
def test_any_other_status_fails_at_once(server: _Server, status: int) -> None:
    server.script = [(status, {"Location": f"{server.url}/elsewhere"}, b"")]
    waits: list[float] = []
    with (
        fetch.HttpGetter(sleep=waits.append) as getter,
        pytest.raises(fetch.FetchError, match=f"answered HTTP {status}"),
    ):
        getter(f"{server.url}/x")
    assert (waits, len(server.requests)) == ([], 1)


def test_a_429_waits_the_retry_after_seconds(
    server: _Server, caplog: pytest.LogCaptureFixture
) -> None:
    server.script = [(429, {"Retry-After": "3"}, b""), (200, {}, b"ok")]
    waits: list[float] = []
    with (
        caplog.at_level(logging.DEBUG, logger=fetch.__name__),
        fetch.HttpGetter(sleep=waits.append) as getter,
    ):
        assert getter(f"{server.url}/x") == b"ok"
    assert waits == [3.0]
    assert caplog.messages == [
        "downloads from 127.0.0.1 connect directly, through no proxy",
        (
            f"GET {server.url}/x answered HTTP 429; Retry-After asks 3.0 s, the cap is 60.0 s; "
            "waiting 3.0 s"
        ),
    ]


@pytest.mark.parametrize(
    ("header", "wait"),
    [("Wed, 21 Oct 2015 07:28:00 GMT", 0.0), ("Fri, 31 Dec 9999 23:59:59 GMT", 60.0)],
)
def test_a_429_waits_until_the_retry_after_date_capped_at_a_minute(
    server: _Server, header: str, wait: float
) -> None:
    server.script = [(429, {"Retry-After": header}, b""), (200, {}, b"ok")]
    waits: list[float] = []
    with fetch.HttpGetter(sleep=waits.append) as getter:
        assert getter(f"{server.url}/x") == b"ok"
    assert waits == [wait]


def test_a_retry_after_beyond_a_minute_is_capped(server: _Server) -> None:
    server.script = [(429, {"Retry-After": "86400"}, b""), (200, {}, b"ok")]
    waits: list[float] = []
    with fetch.HttpGetter(sleep=waits.append) as getter:
        assert getter(f"{server.url}/x") == b"ok"
    assert waits == [60.0]


def test_a_429_without_a_numeric_retry_after_backs_off_then_fails(server: _Server) -> None:
    server.script = [(429, {"Retry-After": "soon"}, b"")] + [(429, {}, b"")] * 5
    waits: list[float] = []
    with (
        fetch.HttpGetter(sleep=waits.append) as getter,
        pytest.raises(fetch.FetchError, match="answered HTTP 429"),
    ):
        getter(f"{server.url}/x")
    assert waits == [2.0, 4.0, 8.0, 16.0, 32.0]
    assert len(server.requests) == 6


def test_a_connection_failure_is_a_fetch_error() -> None:
    with fetch.HttpGetter() as getter, pytest.raises(fetch.FetchError, match="failed"):
        getter(f"http://127.0.0.1:{REFUSED_PORT}/x")


def test_one_pool_serves_each_host(server: _Server) -> None:
    server.script = [(200, {}, b"a"), (200, {}, b"b")]
    with fetch.HttpGetter() as getter:
        getter(f"{server.url}/a")
        getter(f"{server.url}/b")
        assert len(getter._pools) == 1
    assert getter._pools == {}


def test_an_https_proxy_carries_the_request(
    server: _Server, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The local server stands in for the proxy: it receives the absolute URL of the target.
    monkeypatch.setenv("HTTPS_PROXY", server.url)
    server.script = [(200, {}, b"via proxy")]
    with fetch.HttpGetter() as getter:
        assert getter("http://massive.invalid/docs/llms.txt") == b"via proxy"
    assert server.requests[0][0] == "http://massive.invalid/docs/llms.txt"
    assert server.proxy_authorizations == [None]


def test_a_proxy_reached_over_https_is_verified_like_massive(
    server: _Server, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("HTTPS_PROXY", "https://proxy.corp:3129")
    with fetch.HttpGetter() as getter:
        pool = getter._pool("massive.invalid")
        assert isinstance(pool, urllib3.ProxyManager)
        proxy_context = pool.connection_pool_kw["_proxy_config"].ssl_context
        assert proxy_context is not None
        assert proxy_context.verify_mode is ssl.CERT_REQUIRED
        assert proxy_context.check_hostname
        # One context serves both legs: the proxy and the server are verified alike.
        assert proxy_context is pool.connection_pool_kw["ssl_context"]


def test_each_hosts_pool_has_a_tls_context_of_its_own() -> None:
    # urllib3 writes to the context of every connection it opens, so no context is shared.
    with fetch.HttpGetter() as getter:
        contexts = [getter._pool(host).connection_pool_kw["ssl_context"] for host in ("a", "b")]
    assert contexts[0] is not contexts[1]
    assert http.ssl_context() not in contexts


def test_a_tunnel_the_proxy_refuses_names_https_proxy_never_the_password(
    server: _Server, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    port = server.url.rpartition(":")[2]
    monkeypatch.setenv("HTTPS_PROXY", f"http://alice:p%40ss@127.0.0.1:{port}")
    with (
        caplog.at_level(logging.DEBUG, logger=fetch.__name__),
        fetch.HttpGetter() as getter,
        pytest.raises(fetch.FetchError) as caught,
    ):
        getter("https://massive.invalid/docs/llms.txt")
    token = base64.b64encode(b"alice:p@ss").decode("ascii")
    assert server.requests[0][0] == "CONNECT massive.invalid:443"
    assert server.proxy_authorizations == [f"Basic {token}"]
    assert str(caught.value) == (
        "GET https://massive.invalid/docs/llms.txt: the proxy refused its credentials (HTTP 407 "
        "for a tunnel to massive.invalid); check the user and password in HTTPS_PROXY"
    )
    shown = str(caught.value) + str(caught.value.__cause__) + caplog.text
    assert "p@ss" not in shown
    assert "p%40ss" not in shown
    assert caplog.messages == [
        (
            f"downloads from massive.invalid go through the proxy http://127.0.0.1:{port} "
            "(credentials sent: True)"
        )
    ]


def test_a_proxy_user_and_password_go_to_the_proxy_as_proxy_authorization(
    server: _Server, monkeypatch: pytest.MonkeyPatch
) -> None:
    # urllib3 never sends the credentials of a proxy URL itself; a percent-encoded password
    # is decoded first.
    port = server.url.rpartition(":")[2]
    monkeypatch.setenv("HTTPS_PROXY", f"http://alice:p%40ss@127.0.0.1:{port}")
    server.script = [(200, {}, b"via proxy")]
    with fetch.HttpGetter() as getter:
        assert getter("http://massive.invalid/docs/llms.txt") == b"via proxy"
        [pool] = getter._pools.values()
        assert isinstance(pool, urllib3.ProxyManager)
        assert "p@ss" not in str(pool.proxy)
        assert "p%40ss" not in str(pool.proxy)
    token = base64.b64encode(b"alice:p@ss").decode("ascii")
    assert server.proxy_authorizations == [f"Basic {token}"]


def test_fetch_uses_and_closes_an_http_getter_by_default(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    events: list[str] = []
    sources = _sources()

    class Recording:
        """Stands in for `HttpGetter`: answers from the sources and records its closing."""

        def __enter__(self) -> Self:
            return self

        def __exit__(self, *_: object) -> None:
            events.append("closed")

        def __call__(self, url: str) -> bytes:
            events.append(url)
            return sources[url]

    monkeypatch.setattr(fetch, "HttpGetter", Recording)
    fetch.fetch(tmp_path / "s", today=dt.date(2026, 1, 1))
    assert events == [
        fetch.OPENAPI_URL,
        fetch.WEBSOCKET_URL,
        fetch.LLMS_URL,
        _PAGE,
        _FEED,
        _FILE,
        _ARTICLE,
        "closed",
    ]
