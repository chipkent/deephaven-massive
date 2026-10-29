"""A fake Massive REST server: a real HTTP server on 127.0.0.1 that replays scripted replies.

`FakeRestServer` is a `ThreadingHTTPServer` on an ephemeral port, so the REST client is tested
over real sockets, with real headers, connection reuse, resets, proxies, and TLS, and never
reaches Massive. A test scripts the replies per path (`reply`), runs the client, and then reads
what the server received (`requests`).

Replies are matched by the request's full target (path and query) first, then by its path
alone. Each match consumes the first scripted reply of its queue; the last one is kept, so a
path scripted once answers every request the same way. An unscripted path answers 404.

The server also behaves as an HTTP proxy. A `CONNECT host:port` request is recorded (method
`CONNECT`, `proxied=True`) and then tunnelled byte for byte to that address, so a test can put
it in `HTTPS_PROXY` in front of a TLS fake and see exactly what reaches the proxy; after
`refuse_tunnels(status)` it answers every `CONNECT` with that status instead, as a proxy that
rejects the client's credentials (407) does. A request
line with an absolute URL (`GET http://host/path`, forwarding) is answered like any other and
recorded with `proxied=True`.

`REFUSED_PORT` is a loopback port that refuses every connection, for tests of a connection
that fails. `LoopbackHTTPServer`, the server class underneath `FakeRestServer`, also serves
tests that need a plain HTTP server on 127.0.0.1.

Every method is safe to call from any thread: the script and the record sit behind one lock.
Imported lazily by the `fake_rest` fixture in `tests/conftest.py`.
"""

from __future__ import annotations

import gzip
import http.server
import json
import select
import socket
import socketserver
import ssl
import threading
from collections import deque
from dataclasses import dataclass, field
from http import HTTPStatus
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

from deephaven_massive._types import override

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from pathlib import Path


# A loopback port that refuses connections. Port 1 lies below every operating system's range of
# ephemeral ports, so no test that binds port 0, in this process or a parallel test worker, is
# ever handed it, and nothing listens on it. A port bound and released by the test could be
# taken by another worker before the connection is attempted; one kept bound without listening
# is not refused on macOS, where the connection waits out its timeout instead.
REFUSED_PORT = 1


@dataclass(frozen=True)
class Reply:
    """One scripted response.

    Attributes:
        body: The body: `bytes` as is, anything else encoded as JSON.
        status: The HTTP status.
        headers: Extra response headers, such as `{"Retry-After": "5"}`. A `Date` header
            replaces the server's own, which states the real time.
        reset: Close the connection without any response (a connection reset).
        gzip: Compress the body and send `Content-Encoding: gzip`.
        hold: When set, send only the first `hold_after` bytes of the body, then wait (ten
            seconds at most) until the test sets this event before sending the rest, as a
            slow server does. The rest is dropped quietly if the client has closed the
            connection by then.
        hold_after: How many bytes of the (compressed) body `hold` sends before it waits.
    """

    body: object = b""
    status: int = 200
    headers: Mapping[str, str] = field(default_factory=dict)
    reset: bool = False
    gzip: bool = False
    hold: threading.Event | None = None
    hold_after: int = 0

    def payload(self) -> bytes:
        """Return the body's bytes, compressed when `gzip` is set."""
        data = self.body if isinstance(self.body, bytes) else json.dumps(self.body).encode()
        return gzip.compress(data) if self.gzip else data


@dataclass(frozen=True)
class Received:
    """One request the server received.

    Attributes:
        target: The path and query, as the client sent them (a proxied absolute URL is reduced
            to its path and query); for `CONNECT`, the `host:port` tunnelled to.
        headers: The request headers, with lower-case names.
        proxied: Whether the request was meant for a proxy: an absolute URL, or `CONNECT`.
        host_url: For a forwarded request, the scheme and host it named; otherwise `""`.
        method: `GET` or `CONNECT`.
    """

    target: str
    headers: Mapping[str, str]
    proxied: bool = False
    host_url: str = ""
    method: str = "GET"


def envelope(
    results: object = None,
    *,
    status: str = "OK",
    request_id: str = "req-1",
    next_url: str | None = None,
    **extra: object,
) -> dict[str, object]:
    """Return a Massive REST envelope.

    Args:
        results: The rows under `results`; left out when `None`.
        status: The envelope `status`.
        request_id: The envelope `request_id`.
        next_url: The `next_url`; left out when `None`.
        **extra: Other envelope keys, such as `count` or `message`.

    Returns:
        The envelope, ready to pass as a `Reply` body.
    """
    body: dict[str, object] = {"status": status, "request_id": request_id, **extra}
    if results is not None:
        body["results"] = results
    if next_url is not None:
        body["next_url"] = next_url
    return body


class LoopbackHTTPServer(http.server.ThreadingHTTPServer):
    """A `ThreadingHTTPServer` that binds without a reverse DNS lookup of its address.

    `HTTPServer.server_bind` names the server with `socket.getfqdn(host)`, a reverse lookup that
    costs a millisecond or more per server, and much more where DNS is slow. No test reads that
    name, so the address itself stands in for it. `FakeRestServer` and any test that needs a
    plain HTTP server on 127.0.0.1 use this class.
    """

    @override
    def server_bind(self) -> None:
        socketserver.TCPServer.server_bind(self)
        host, port = self.server_address[:2]
        self.server_name = str(host)
        self.server_port = int(port)


class FakeRestServer:
    """A scripted HTTP server speaking Massive's REST envelope; see the module docstring.

    Create it, `start()` it, script it with `reply`, and `close()` it; the `fake_rest` fixture
    does the first and last. Optionally serve TLS with a certificate and key.
    """

    def __init__(self, tls: tuple[Path, Path] | None = None) -> None:
        """Bind the server to an ephemeral port on 127.0.0.1; it serves after `start()`.

        Args:
            tls: The certificate and key files to serve HTTPS with, or `None` for HTTP.
        """
        self._lock = threading.Lock()
        self._script: dict[str, deque[Reply]] = {}
        self._received: list[Received] = []
        self._on_request: Callable[[Received], None] | None = None
        self._tunnel_status: int | None = None
        self._server = LoopbackHTTPServer(("127.0.0.1", 0), self._handler())
        self._server.daemon_threads = True
        self._scheme = "http"
        if tls is not None:
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            context.load_cert_chain(tls[0], tls[1])
            self._server.socket = context.wrap_socket(self._server.socket, server_side=True)
            self._scheme = "https"
        self._thread = threading.Thread(
            target=self._server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True
        )

    @property
    def url(self) -> str:
        """The base URL, such as `"http://127.0.0.1:54321"`."""
        host, port = self._server.server_address[:2]
        return f"{self._scheme}://{host!s}:{port}"

    def start(self) -> None:
        """Start serving on a background thread."""
        self._thread.start()

    def close(self) -> None:
        """Stop serving and release the port."""
        self._server.shutdown()
        self._server.server_close()
        self._thread.join()

    def reply(self, target: str, *replies: Reply) -> None:
        """Script replies for a path, or for one exact path and query.

        Args:
            target: A path (`"/v3/trades/AAPL"`) or a path and query.
            *replies: The replies, in order; the last repeats.
        """
        with self._lock:
            self._script.setdefault(target, deque()).extend(replies)

    def reply_first(self, target: str, *replies: Reply) -> None:
        """Script replies to be used before those already scripted for `target`.

        Args:
            target: A path, or a path and query, already scripted with `reply`.
            *replies: The replies, in order, ahead of the existing ones.
        """
        with self._lock:
            self._script[target].extendleft(reversed(replies))

    def on_request(self, hook: Callable[[Received], None] | None) -> None:
        """Run `hook` on the server's thread as each request arrives, before it is answered.

        Args:
            hook: The hook, or `None` to remove it.
        """
        with self._lock:
            self._on_request = hook

    def refuse_tunnels(self, status: int) -> None:
        """Answer every later `CONNECT` with `status` and open no tunnel.

        Args:
            status: The status, such as 407 for a proxy that rejects the client's credentials;
                a 407 carries a `Proxy-Authenticate: Basic` challenge, as a real proxy's does.
        """
        with self._lock:
            self._tunnel_status = status

    @property
    def requests(self) -> list[Received]:
        """A copy of every request received so far, in arrival order."""
        with self._lock:
            return list(self._received)

    def _record(self, received: Received) -> None:
        """Record a request that gets no scripted reply (a `CONNECT`).

        Args:
            received: The request.
        """
        with self._lock:
            self._received.append(received)

    def _next(self, received: Received) -> tuple[Reply, Callable[[Received], None] | None]:
        """Record a request and choose its reply.

        Args:
            received: The request.

        Returns:
            The reply, and the hook to run.
        """
        path = received.target.partition("?")[0]
        with self._lock:
            self._received.append(received)
            queue = self._script.get(received.target) or self._script.get(path)
            if not queue:
                reply = Reply(b'{"status":"NOT_FOUND"}', status=404)
            elif len(queue) > 1:
                reply = queue.popleft()
            else:
                reply = queue[0]
            return reply, self._on_request

    def _handler(self) -> type[http.server.BaseHTTPRequestHandler]:
        """Return the request handler class bound to this server.

        Returns:
            The class.
        """
        fake = self

        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_GET(self) -> None:
                parts = urlsplit(self.path)
                proxied = bool(parts.scheme)
                target = f"{parts.path}?{parts.query}" if parts.query else parts.path
                received = Received(
                    target=target,
                    headers={name.lower(): value for name, value in self.headers.items()},
                    proxied=proxied,
                    host_url=f"{parts.scheme}://{parts.netloc}" if proxied else "",
                )
                reply, hook = fake._next(received)
                if hook is not None:
                    hook(received)
                if reply.reset:
                    self.close_connection = True
                    return
                data = reply.payload()
                if "Date" in reply.headers:  # the scripted date replaces the real one
                    self.send_response_only(reply.status)
                    self.send_header("Server", self.version_string())
                else:
                    self.send_response(reply.status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                if reply.gzip:
                    self.send_header("Content-Encoding", "gzip")
                for name, value in reply.headers.items():
                    self.send_header(name, value)
                self.end_headers()
                if reply.hold is None:
                    self.wfile.write(data)
                    return
                self.wfile.write(data[: reply.hold_after])
                self.wfile.flush()
                reply.hold.wait(10)
                # The client may have hung up while the tail waited: send the tail if it can,
                # and never read another request from this connection.
                self.close_connection = True
                try:
                    self.wfile.write(data[reply.hold_after :])
                    self.wfile.flush()
                except (BrokenPipeError, ConnectionResetError):
                    pass

            def do_CONNECT(self) -> None:
                fake._record(
                    Received(
                        target=self.path,
                        headers={name.lower(): value for name, value in self.headers.items()},
                        proxied=True,
                        method="CONNECT",
                    )
                )
                with fake._lock:
                    refusal = fake._tunnel_status
                if refusal is not None:
                    self.send_response(refusal)
                    if refusal == HTTPStatus.PROXY_AUTHENTICATION_REQUIRED:
                        self.send_header("Proxy-Authenticate", 'Basic realm="proxy"')
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    self.close_connection = True
                    return
                host, _, port = self.path.rpartition(":")
                with socket.create_connection((host, int(port)), timeout=10) as upstream:
                    self.send_response(200, "Connection established")
                    self.end_headers()
                    _relay(self.connection, upstream)
                self.close_connection = True

        return Handler


def _relay(client: socket.socket, upstream: socket.socket) -> None:
    """Copy bytes both ways between two sockets until either side closes.

    Args:
        client: The proxy's connection from the client.
        upstream: The connection to the tunnelled address.
    """
    sockets = [client, upstream]
    while True:
        readable, _, _ = select.select(sockets, [], [], 10)
        if not readable:
            return
        for source in readable:
            data = source.recv(65536)
            if not data:
                return
            (upstream if source is client else client).sendall(data)
