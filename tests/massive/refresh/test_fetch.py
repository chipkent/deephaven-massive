from __future__ import annotations

import datetime as dt
import hashlib
import json
import socket
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING, Self

import pytest

from deephaven_massive._massive.net import USER_AGENT
from deephaven_massive._massive.refresh import fetch

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from pathlib import Path

# --- Canonical forms ------------------------------------------------------------------------


def test_canonical_json_sorts_indents_and_ends_with_a_newline() -> None:
    raw = '{"b": [1, {"d": "é", "c": null}], "a": true}'.encode()
    assert (
        fetch.canonical_json(raw, "u")
        == ('{\n "a": true,\n "b": [\n  1,\n  {\n   "c": null,\n   "d": "é"\n  }\n ]\n}\n').encode()
    )


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
_INDEX = f"# Massive\n\n## REST API\n\n- [Trades]({_PAGE}): Trades   \n"
_SPEC = b'{"paths": {}, "openapi": "3.0.3"}'


def _sources(**overrides: bytes) -> dict[str, bytes]:
    sources = {
        fetch.OPENAPI_URL: _SPEC,
        fetch.LLMS_URL: _INDEX.encode(),
        _PAGE: b"**Endpoint:** `GET /v3/trades/{stockTicker}`  \n",
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
    assert requested == [fetch.OPENAPI_URL, fetch.LLMS_URL, _PAGE]
    assert names == [
        "openapi.json",
        "llms.txt",
        "rest/stocks/trades-quotes/trades.md",
        "manifest.json",
    ]
    stored_spec = (target / "openapi.json").read_bytes()
    assert stored_spec == b'{\n "openapi": "3.0.3",\n "paths": {}\n}\n'
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
    assert set(manifest["files"]) == set(names[:-1])
    assert (target / "manifest.json").read_text(encoding="utf-8").endswith("}\n")


def test_fetch_replaces_the_old_snapshot_entirely(tmp_path: Path) -> None:
    target = tmp_path / "snapshot"
    (target / "rest" / "gone").mkdir(parents=True)
    (target / "rest" / "gone" / "page.md").write_text("removed by Massive\n", encoding="utf-8")
    (tmp_path / "snapshot-incoming").mkdir()  # a staging directory left by an interrupted run
    fetch.fetch(target, get=_getter(_sources()), today=dt.date(2026, 1, 1))
    assert not (target / "rest" / "gone").exists()
    assert not (tmp_path / "snapshot-incoming").exists()


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


def test_an_index_linking_outside_the_rest_documentation_fails(tmp_path: Path) -> None:
    index = b"## REST API\n- [Evil](https://example.com/x.md)\n"
    with pytest.raises(fetch.FetchError, match="not a REST documentation page"):
        fetch.fetch(tmp_path / "s", get=_getter(_sources(**{fetch.LLMS_URL: index})))


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
        server = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                server.requests.append((self.path, self.headers.get("User-Agent", "")))
                status, headers, body = server.script.pop(0)
                self.send_response(status)
                for name, value in headers.items():
                    self.send_header(name, value)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        self.http = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
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


def test_a_429_waits_the_retry_after_seconds(server: _Server) -> None:
    server.script = [(429, {"Retry-After": "3"}, b""), (200, {}, b"ok")]
    waits: list[float] = []
    with fetch.HttpGetter(sleep=waits.append) as getter:
        assert getter(f"{server.url}/x") == b"ok"
    assert waits == [3.0]


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
    with socket.socket() as probe:  # a port nothing listens on once the probe closes
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    with fetch.HttpGetter() as getter, pytest.raises(fetch.FetchError, match="failed"):
        getter(f"http://127.0.0.1:{port}/x")


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
    assert events == [fetch.OPENAPI_URL, fetch.LLMS_URL, _PAGE, "closed"]
