"""Tests for `WsClient` against the fake Massive WebSocket server.

Each client runs on its own thread, as in production. The tests synchronize with it through
conditions (the recorder's and the fake server's), never by sleeping: backoff waits go through
an injected `Sleeper` that records each wait and either returns at once or holds until
`close()` cancels it, and time comes from the shared `FakeClock` (`tests/_fakes/clock.py`),
whose monotonic time moves only when a test advances it.
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import pytest

from deephaven_massive._massive import WireFailure
from deephaven_massive._massive.net import USER_AGENT
from deephaven_massive._massive.net.errors import WireAuthError, WireError
from deephaven_massive._massive.spec.types import (
    DriftReport,
    DriftSample,
    Field,
    RowFault,
    Source,
    Transport,
    WireType,
    WireUnit,
)
from deephaven_massive._massive.ws import client as client_module
from deephaven_massive._massive.ws.client import WsClient, WsState
from deephaven_massive._massive.ws.protocol import Status, StatusKind

# Imported directly, as the REST client's tests do: each client needs its own clock, built
# with the client inside the factory fixture below, which a shared fixture cannot provide.
from tests._fakes.clock import FakeClock
from tests._fakes.ws import NoReply

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

    from tests._fakes.ws import FakeMassiveServer

KEY = "test-api-key-1234"

SOURCES = (
    Source(
        key="stocks/T",
        transport=Transport.WS,
        fields=(
            Field(wire_key="sym", column="Ticker", wire_type=WireType.STRING),
            Field(wire_key="p", column="Price", wire_type=WireType.DOUBLE),
            # Marked required by the vendor, so an object without it is reported as drift.
            Field(wire_key="s", column="Size", wire_type=WireType.LONG, required=True),
            Field(wire_key="t", column="SipTimestamp", wire_type=WireType.EPOCH, unit=WireUnit.MS),
        ),
    ),
    Source(
        key="stocks/A",
        transport=Transport.WS,
        fields=(
            Field(wire_key="sym", column="Ticker", wire_type=WireType.STRING),
            Field(wire_key="c", column="Close", wire_type=WireType.DOUBLE),
        ),
    ),
    Source(
        key="stocks/AM",
        transport=Transport.WS,
        fields=(
            Field(wire_key="sym", column="Ticker", wire_type=WireType.STRING),
            Field(wire_key="c", column="Close", wire_type=WireType.DOUBLE),
        ),
    ),
)


def _status(status: str, message: str) -> str:
    return json.dumps([{"ev": "status", "status": status, "message": message}])


class Recorder:
    """Records every callback, in order, and lets the test thread wait for one."""

    def __init__(self) -> None:
        self._cond = threading.Condition()
        self.events: list[tuple[Any, ...]] = []
        self.threads: set[int] = set()
        self.open_hook: Callable[[int], None] | None = None
        self.state_hook: Callable[[WsState], None] | None = None

    def _add(self, event: tuple[Any, ...]) -> None:
        with self._cond:
            self.events.append(event)
            self.threads.add(threading.get_ident())
            self._cond.notify_all()

    def on_open(self, generation: int) -> None:
        self._add(("open", generation))
        if self.open_hook is not None:
            self.open_hook(generation)

    def on_status(self, generation: int, status: Status) -> None:
        self._add(("status", generation, status))

    def on_data(self, *args: object) -> None:
        # The arguments are the generation, channel, rows, marks, and drift.
        self._add(("data", *args))

    def on_state(self, state: WsState, detail: float | WireError | None) -> None:
        self._add(("state", state, detail))
        if self.state_hook is not None:
            self.state_hook(state)

    def wait_for(self, predicate: Callable[[list[tuple[Any, ...]]], bool]) -> None:
        with self._cond:
            assert self._cond.wait_for(lambda: predicate(self.events), 10.0), self.events

    def wait_state(self, state: WsState) -> None:
        self.wait_for(lambda events: bool(events) and events[-1][:2] == ("state", state))

    def wait_open(self, generation: int) -> None:
        self.wait_for(lambda events: ("open", generation) in events)

    def of(self, kind: str) -> list[tuple[Any, ...]]:
        with self._cond:
            return [e[1:] for e in self.events if e[0] == kind]

    def states(self) -> list[WsState]:
        return [state for state, _ in self.of("state")]

    def count(self, kind: str, value: object = None) -> Callable[[list[tuple[Any, ...]]], int]:
        return lambda events: sum(
            1 for e in events if e[0] == kind and (value is None or e[1] == value)
        )


class Sleeper:
    """The injected backoff wait: records each wait; returns at once for the first `free`."""

    def __init__(self, free: int) -> None:
        self._cond = threading.Condition()
        self.waits: list[float] = []
        self.free = free

    async def __call__(self, delay: float) -> None:
        with self._cond:
            self.waits.append(delay)
            held = len(self.waits) > self.free
            self._cond.notify_all()
        if held:
            await asyncio.sleep(3600)  # until close() cancels it

    def wait_for_count(self, count: int) -> list[float]:
        with self._cond:
            assert self._cond.wait_for(lambda: len(self.waits) >= count, 10.0), self.waits
            return list(self.waits)


@dataclass
class Harness:
    client: WsClient
    recorder: Recorder
    sleeper: Sleeper
    clock: FakeClock

    def on_loop(self, action: Callable[[], object]) -> object:
        # Run `action` on the client's loop thread; its result or exception comes back here.
        async def run() -> object:
            return action()

        return asyncio.run_coroutine_threadsafe(run(), self.client.loop).result(10.0)


@pytest.fixture
def make_client(fake_ws_server: FakeMassiveServer) -> Iterator[Callable[..., Harness]]:
    made: list[WsClient] = []

    def make(
        *,
        url: str | None = None,
        free_waits: int = 100,
        backoff: tuple[float, float] = (1.0, 1.0),
        start: bool = True,
        recorder: Recorder | None = None,
        reconnect_deadline: Callable[[], float | None] = lambda: None,
    ) -> Harness:
        recorder = recorder or Recorder()
        sleeper, clock = Sleeper(free_waits), FakeClock()
        client = WsClient(
            url or fake_ws_server.url(),
            api_key=KEY,
            sources=SOURCES,
            on_open=recorder.on_open,
            on_status=recorder.on_status,
            on_data=recorder.on_data,
            on_state=recorder.on_state,
            reconnect_backoff=backoff[0],
            max_reconnect_backoff=backoff[1],
            reconnect_deadline=reconnect_deadline,
            clock=clock,
            sleep=sleeper,
        )
        made.append(client)
        if start:
            client.start()
            client.connect()
        return Harness(client, recorder, sleeper, clock)

    yield make
    for client in made:
        client.close()
    assert all(client.join(10.0) for client in made)


# --- connecting and authenticating ----------------------------------------------------------


def test_it_authenticates_and_opens_generation_one(
    make_client: Callable[..., Harness], fake_ws_server: FakeMassiveServer
) -> None:
    h = make_client()
    h.recorder.wait_open(1)
    connection = fake_ws_server.connections[0]
    assert json.loads(connection.auth or "") == {"action": "auth", "params": KEY}
    assert connection.path == "/stocks"
    assert connection.user_agent == USER_AGENT
    assert h.recorder.states() == [WsState.CONNECTING]
    assert h.recorder.threads == {h.client._thread.ident}
    # A client used alone names its thread with the default prefix.
    assert h.client._thread.name == "massive ws 127.0.0.1/stocks"


def _log_a_session(make_client: Callable[..., Harness], caplog: pytest.LogCaptureFixture) -> str:
    # Open a generation, subscribe, and close, capturing every logger at DEBUG.
    caplog.set_level(logging.DEBUG)  # the root, so websockets' own loggers are captured too
    h = make_client()
    h.recorder.wait_open(1)
    h.on_loop(lambda: h.client.subscribe(1, ["T.AAPL"]))
    h.recorder.wait_for(lambda e: any(x[0] == "status" for x in e))
    h.client.close()
    assert h.client.join(10.0)
    return "\n".join(f"{r.name} {r.getMessage()}" for r in caplog.records)


def test_the_key_is_never_logged_by_anyone(
    make_client: Callable[..., Harness], caplog: pytest.LogCaptureFixture
) -> None:
    text = _log_a_session(make_client, caplog)
    assert "connecting to" in text  # the capture did see the client's records
    assert KEY not in text


def test_each_status_is_logged_at_debug(
    make_client: Callable[..., Harness],
    fake_ws_server: FakeMassiveServer,
    caplog: pytest.LogCaptureFixture,
) -> None:
    text = _log_a_session(make_client, caplog)
    url = fake_ws_server.url()
    assert f"{client_module.__name__} {url} status ACK: subscribed to: T.AAPL" in text


def test_websockets_would_log_the_key_without_the_quiet_logger(
    make_client: Callable[..., Harness],
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Proves the test above can fail: websockets' own logger logs the authentication frame.
    monkeypatch.setattr(client_module, "_WIRE_LOGGER", logging.getLogger("websockets.client"))
    assert KEY in _log_a_session(make_client, caplog)


def test_auth_failed_is_fatal_and_never_retried(
    make_client: Callable[..., Harness], fake_ws_server: FakeMassiveServer
) -> None:
    fake_ws_server.auth_replies.append("auth_failed")
    h = make_client()
    h.recorder.wait_state(WsState.FAILED)
    assert h.client.join(10.0)
    [(_, error)] = [s for s in h.recorder.of("state") if s[0] is WsState.FAILED]
    assert isinstance(error, WireAuthError)
    assert error.host == "127.0.0.1"
    assert error.path == "/stocks"
    assert "authentication failed" in error.message
    assert len(fake_ws_server.connections) == 1
    assert h.sleeper.waits == []
    assert h.recorder.of("open") == []


def test_max_connections_waits_for_a_slot_then_retries(
    make_client: Callable[..., Harness], fake_ws_server: FakeMassiveServer
) -> None:
    fake_ws_server.auth_replies.append("max_connections")
    h = make_client(backoff=(1.0, 1.0))
    h.recorder.wait_open(1)
    assert h.recorder.states() == [WsState.CONNECTING, WsState.WAITING_SLOT, WsState.CONNECTING]
    [(_, at)] = [s for s in h.recorder.of("state") if s[0] is WsState.WAITING_SLOT]
    assert at == h.clock.time() + 1.0
    assert h.sleeper.waits == [1.0]
    assert len(fake_ws_server.connections) == 2


def test_max_connections_logs_at_debug_on_every_attempt(
    make_client: Callable[..., Harness],
    fake_ws_server: FakeMassiveServer,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Every attempt of an outage gets this answer, so it never warns; the library's
    # connection warns once per outage instead.
    caplog.set_level(logging.DEBUG, logger=client_module.__name__)
    fake_ws_server.auth_replies.extend(["max_connections", "max_connections"])
    h = make_client()
    h.recorder.wait_open(1)
    refusals = [r for r in caplog.records if "no free connection" in r.getMessage()]
    assert [r.levelno for r in refusals] == [logging.DEBUG, logging.DEBUG]
    assert refusals[0].getMessage() == (
        f"{fake_ws_server.url()} refused the connection: the account has no free connection "
        "(max_connections)"
    )
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


def test_max_connections_without_a_close_ends_at_the_handshake_timeout(
    make_client: Callable[..., Harness],
    fake_ws_server: FakeMassiveServer,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(client_module, "HANDSHAKE_TIMEOUT", 0.2)
    fake_ws_server.auth_replies.append("max_connections")
    fake_ws_server.close_after_refusal = False
    h = make_client(free_waits=0)
    h.sleeper.wait_for_count(1)
    assert h.recorder.states() == [WsState.CONNECTING, WsState.WAITING_SLOT]


def test_a_silent_server_is_cut_off_by_the_handshake_timeout(
    *,
    make_client: Callable[..., Harness],
    fake_ws_server: FakeMassiveServer,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # The server greets and reads the key, then never answers. The attempt's deadline is read
    # from the client loop's clock, which runs true until the server holds the key, so the
    # upgrade and the greeting have all the time they need; then it jumps past the deadline,
    # so the timeout cuts off the silent server at once. The held backoff wait stops a second
    # attempt.
    caplog.set_level(logging.DEBUG, logger=client_module.__name__)
    fake_ws_server.auth_replies.append(NoReply.TO_KEY)
    h = make_client(free_waits=0, start=False)
    loop = h.client.loop
    true_time = loop.time
    late = threading.Event()

    def skewed() -> float:
        return true_time() + (2 * client_module.HANDSHAKE_TIMEOUT if late.is_set() else 0.0)

    monkeypatch.setattr(loop, "time", skewed)
    h.client.start()
    h.client.connect()
    fake_ws_server.wait_for(lambda s: bool(s.connections) and s.connections[0].auth is not None)
    late.set()
    loop.call_soon_threadsafe(lambda: None)  # wake the loop to see its clock's jump
    h.sleeper.wait_for_count(1)
    assert h.recorder.states() == [WsState.CONNECTING, WsState.RECONNECTING]
    assert len(fake_ws_server.connections) == 1
    # Each attempt logs at DEBUG; the library's connection logs one WARNING per outage.
    assert any(
        r.levelno == logging.DEBUG
        and "handshake" in r.getMessage()
        and "TimeoutError" in r.getMessage()
        for r in caplog.records
    )


@pytest.fixture(
    params=[
        ("greeting", _status("auth_success", "early")),  # no `connected` greeting first
        ("greeting", "not json"),
        ("reply", "auth_timeout"),  # an unknown reply to the key
    ],
    ids=["no-greeting", "not-json", "unknown-reply"],
)
def drifted_handshake(request: pytest.FixtureRequest, fake_ws_server: FakeMassiveServer) -> None:
    """Script the fake's next handshake to break Massive's protocol one way."""
    step, value = request.param
    if step == "greeting":
        fake_ws_server.greet_next(value)
    else:
        fake_ws_server.auth_replies.append(value)


@pytest.mark.parametrize(
    "reply",
    [
        ("auth_timeout", f"echoes {KEY}"),  # an unknown reply that quotes the key
        json.dumps([{"ev": "other", "echo": KEY}]).encode(),  # no status object at all
    ],
    ids=["unknown-reply", "no-status"],
)
def test_a_broken_reply_that_echoes_the_key_is_logged_without_it(
    *,
    reply: tuple[str, str] | bytes,
    make_client: Callable[..., Harness],
    fake_ws_server: FakeMassiveServer,
    caplog: pytest.LogCaptureFixture,
) -> None:
    fake_ws_server.auth_replies.append(reply)
    h = make_client()
    h.recorder.wait_open(1)
    [record] = [r for r in caplog.records if "broke protocol" in r.getMessage()]
    assert record.exc_info is not None
    text = logging.Formatter().format(record)
    assert "***" in text
    assert KEY not in text


@pytest.mark.usefixtures("drifted_handshake")
def test_a_handshake_that_breaks_protocol_is_logged_and_retried(
    make_client: Callable[..., Harness],
    fake_ws_server: FakeMassiveServer,
    caplog: pytest.LogCaptureFixture,
) -> None:
    h = make_client()
    h.recorder.wait_open(1)
    assert h.recorder.states() == [WsState.CONNECTING, WsState.RECONNECTING, WsState.CONNECTING]
    assert len(fake_ws_server.connections) == 2
    [record] = [r for r in caplog.records if "broke protocol" in r.getMessage()]
    assert record.levelno == logging.ERROR


# --- failures while opening the socket ------------------------------------------------------


def test_an_http_404_is_fatal_and_names_the_host(
    make_client: Callable[..., Harness], fake_ws_server: FakeMassiveServer
) -> None:
    fake_ws_server.reject_next(404)
    h = make_client()
    h.recorder.wait_state(WsState.FAILED)
    error = h.recorder.of("state")[-1][1]
    assert type(error) is WireError
    assert error.status == 404
    assert "127.0.0.1/stocks" in error.message
    assert h.sleeper.waits == []


def test_an_http_503_is_retried_after_the_backoff(
    make_client: Callable[..., Harness], fake_ws_server: FakeMassiveServer
) -> None:
    fake_ws_server.reject_next(503)
    h = make_client(backoff=(2.0, 2.0))
    h.recorder.wait_open(1)
    assert h.sleeper.waits == [2.0]
    assert h.recorder.states() == [WsState.CONNECTING, WsState.RECONNECTING, WsState.CONNECTING]


@pytest.mark.parametrize(
    "case",
    [
        ("5", (2.0, 10.0), 5.0),  # Retry-After is longer than the backoff
        ("1", (2.0, 2.0), 2.0),  # the backoff is longer than Retry-After
        ("500", (2.0, 10.0), 10.0),  # Retry-After is capped at max_reconnect_backoff
    ],
    ids=["retry-after", "backoff", "capped"],
)
def test_an_http_429_waits_the_longer_of_retry_after_and_the_backoff(
    make_client: Callable[..., Harness],
    fake_ws_server: FakeMassiveServer,
    case: tuple[str, tuple[float, float], float],
) -> None:
    retry_after, backoff, wait = case
    fake_ws_server.reject_next(429, {"Retry-After": retry_after})
    h = make_client(backoff=backoff)
    h.recorder.wait_open(1)
    assert h.sleeper.waits == [wait]
    [(_, at)] = [s for s in h.recorder.of("state") if s[0] is WsState.RECONNECTING]
    assert at == h.clock.time() + wait


@pytest.mark.parametrize(
    ("deadline", "longest", "shortest"),
    [
        (1000.0, 6.0, 5.0),  # far off: the backoff's wait, 5 s and up to 20% of jitter
        (15.5, 5.5, 5.0),  # an attempt after 5.5 s that takes the whole 10 s still meets it
        (12.0, 5.0, 5.0),  # too close to meet: never below the initial backoff
    ],
    ids=["far", "clamped", "floor"],
)
def test_a_reconnect_wait_is_shortened_so_the_attempt_meets_its_deadline(
    *,
    make_client: Callable[..., Harness],
    fake_ws_server: FakeMassiveServer,
    caplog: pytest.LogCaptureFixture,
    deadline: float,
    longest: float,
    shortest: float,
) -> None:
    caplog.set_level(logging.DEBUG, logger=client_module.__name__)
    fake_ws_server.reject_next(503)
    h = make_client(backoff=(5.0, 120.0), reconnect_deadline=lambda: deadline)
    h.recorder.wait_open(1)
    [wait] = h.sleeper.waits
    assert shortest <= wait <= longest
    shortened = [r.getMessage() for r in caplog.records if "instead of" in r.getMessage()]
    assert all(
        m.endswith(f"so the next attempt can authenticate within {deadline:.1f} s")
        for m in shortened
    )


def test_a_shortened_wait_still_honours_retry_after(
    make_client: Callable[..., Harness], fake_ws_server: FakeMassiveServer
) -> None:
    fake_ws_server.reject_next(429, {"Retry-After": "8"})
    h = make_client(backoff=(2.0, 10.0), reconnect_deadline=lambda: 12.0)
    h.recorder.wait_open(1)
    assert h.sleeper.waits == [8.0]


def test_a_capped_retry_after_is_logged(
    make_client: Callable[..., Harness],
    fake_ws_server: FakeMassiveServer,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG, logger=client_module.__name__)
    fake_ws_server.reject_next(429, {"Retry-After": "500"})
    h = make_client(backoff=(2.0, 10.0))
    h.recorder.wait_open(1)
    [record] = [r for r in caplog.records if "waiting at most" in r.getMessage()]
    assert record.levelno == logging.DEBUG  # a reconnect attempt's detail
    assert record.getMessage().endswith("wait 500.0 s (Retry-After); waiting at most 10.0 s")


def test_a_refused_connection_is_retried(
    make_client: Callable[..., Harness],
    fake_ws_server: FakeMassiveServer,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG, logger=client_module.__name__)
    port = fake_ws_server.port
    fake_ws_server.stop()  # nothing listens on the port any more
    url = f"wss://127.0.0.1:{port}/stocks"
    h = make_client(url=url, free_waits=1)
    assert h.sleeper.wait_for_count(2) == [1.0, 1.0]
    assert WsState.FAILED not in h.recorder.states()
    # Each attempt repeats these lines while the outage lasts, so they stay at DEBUG.
    attempts = [
        r
        for r in caplog.records
        if r.getMessage() == f"connecting to {url}"
        or r.getMessage().startswith(f"could not connect to {url}: ")
    ]
    assert len(attempts) >= 4
    assert {r.levelno for r in attempts} == {logging.DEBUG}


def test_a_url_must_be_a_websocket_url() -> None:
    for url in ("https://socket.massive.com/stocks", "wss:///stocks"):
        with pytest.raises(ValueError, match="not a WebSocket URL"):
            WsClient(
                url,
                api_key=KEY,
                sources=SOURCES,
                on_open=print,
                on_status=print,
                on_data=print,
                on_state=print,
                reconnect_backoff=1.0,
                max_reconnect_backoff=1.0,
            )


# --- reconnecting -----------------------------------------------------------------------------


def test_a_clean_server_close_reconnects_and_drops_old_frames(
    make_client: Callable[..., Harness], fake_ws_server: FakeMassiveServer
) -> None:
    h = make_client()
    h.recorder.wait_open(1)
    fake_ws_server.close(1000)
    h.recorder.wait_open(2)
    assert h.recorder.states() == [
        WsState.CONNECTING,
        WsState.RECONNECTING,
        WsState.CONNECTING,
    ]
    assert fake_ws_server.connections[0].close_code == 1000
    h.on_loop(lambda: h.client.subscribe(1, ["T.OLD"]))
    h.on_loop(lambda: h.client.subscribe(2, ["T.NEW"]))
    fake_ws_server.wait_for(lambda s: len(s.connections[1].frames) == 1)
    assert fake_ws_server.frames() == ['{"action":"subscribe","params":"T.NEW"}']


def test_a_close_is_logged_with_its_code_reason_and_uptime(
    make_client: Callable[..., Harness],
    fake_ws_server: FakeMassiveServer,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, logger=client_module.__name__)
    h = make_client()
    h.recorder.wait_open(1)
    h.clock.advance(12.5)
    fake_ws_server.close(1001, f"going away; key {KEY}")
    h.recorder.wait_open(2)
    [record] = [r for r in caplog.records if " closed (code " in r.getMessage()]
    assert record.levelno == logging.INFO
    # The reason is the server's text, so a key it echoes is replaced.
    assert record.getMessage() == (
        f"the connection to {fake_ws_server.url()} closed (code 1001, reason "
        "'going away; key ***') after 12.5 s up"
    )


def test_an_abnormal_server_close_reconnects(
    make_client: Callable[..., Harness], fake_ws_server: FakeMassiveServer
) -> None:
    h = make_client()
    h.recorder.wait_open(1)
    fake_ws_server.close(1011)
    h.recorder.wait_open(2)
    assert h.sleeper.waits == [1.0]


def test_the_backoff_doubles_and_resets_only_after_a_stable_connection(
    make_client: Callable[..., Harness], fake_ws_server: FakeMassiveServer
) -> None:
    h = make_client(backoff=(1.0, 100.0))
    for generation in (1, 2, 3):
        h.recorder.wait_open(generation)
        if generation == 3:
            h.clock.advance(60.0)  # this one stays up long enough to count as stable
        fake_ws_server.close(1000)
    h.recorder.wait_open(4)
    first, second, after_stable = h.sleeper.waits
    assert 1.0 <= first <= 1.2
    assert 2.0 <= second <= 2.4
    assert 1.0 <= after_stable <= 1.2


def test_auth_success_alone_does_not_reset_the_backoff(
    make_client: Callable[..., Harness], fake_ws_server: FakeMassiveServer
) -> None:
    h = make_client(backoff=(1.0, 100.0))
    for generation in (1, 2):
        h.recorder.wait_open(generation)
        h.clock.advance(59.0)
        fake_ws_server.close(1000)
    h.recorder.wait_open(3)
    assert 2.0 <= h.sleeper.waits[1] <= 2.4


# --- frames -----------------------------------------------------------------------------------


def test_subscribe_sends_at_once_and_the_ack_is_decoded(
    make_client: Callable[..., Harness], fake_ws_server: FakeMassiveServer
) -> None:
    recorder = Recorder()
    h = make_client(start=False, recorder=recorder)
    recorder.open_hook = lambda g: h.client.subscribe(g, ["T.AAPL", "Q.AAPL"])
    h.client.start()
    h.client.connect()
    recorder.wait_for(lambda e: recorder.count("status")(e) == 2)
    assert recorder.of("status") == [
        (1, Status(kind=StatusKind.ACK, topics=("T.AAPL",), message="subscribed to: T.AAPL")),
        (1, Status(kind=StatusKind.ACK, topics=("Q.AAPL",), message="subscribed to: Q.AAPL")),
    ]


def test_a_combined_ack_lists_every_topic(
    make_client: Callable[..., Harness], fake_ws_server: FakeMassiveServer
) -> None:
    fake_ws_server.combined_acks = True
    h = make_client()
    h.recorder.wait_open(1)
    h.on_loop(lambda: h.client.subscribe(1, ["T.AAPL", "Q.AAPL"]))
    h.recorder.wait_for(lambda e: h.recorder.count("status")(e) == 1)
    assert h.recorder.of("status")[0][1].topics == ("T.AAPL", "Q.AAPL")


def test_a_max_connections_status_then_a_close_reports_the_connection_taken(
    make_client: Callable[..., Harness],
    fake_ws_server: FakeMassiveServer,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # A newer connection with the key took this one's place: distinct from a refusal at the
    # key, so the library can decide what it means.
    caplog.set_level(logging.INFO, logger=client_module.__name__)
    h = make_client(free_waits=0)
    h.recorder.wait_open(1)
    fake_ws_server.evict()
    h.sleeper.wait_for_count(1)
    assert h.recorder.of("status")[0][1].kind is StatusKind.MAX_CONNECTIONS
    assert h.recorder.states() == [WsState.CONNECTING, WsState.TAKEN]
    [(_, at)] = [s for s in h.recorder.of("state") if s[0] is WsState.TAKEN]
    assert at == h.clock.time() + h.sleeper.waits[0]
    taken = [r for r in caplog.records if "took this one's place" in r.getMessage()]
    assert [(r.levelno, r.getMessage()) for r in taken] == [
        (
            logging.INFO,
            (
                f"{fake_ws_server.url()}: another connection with this API key took this one's "
                "place (a max_connections status, then the close)"
            ),
        )
    ]


def test_a_taken_connection_reconnects_after_its_backoff(
    make_client: Callable[..., Harness], fake_ws_server: FakeMassiveServer
) -> None:
    h = make_client(backoff=(1.0, 1.0))
    h.recorder.wait_open(1)
    fake_ws_server.evict()
    h.recorder.wait_open(2)  # the reconnect takes the connection back
    assert h.recorder.states() == [WsState.CONNECTING, WsState.TAKEN, WsState.CONNECTING]


def test_subscribe_splits_large_sets_into_frames_of_1000(
    make_client: Callable[..., Harness], fake_ws_server: FakeMassiveServer
) -> None:
    h = make_client()
    h.recorder.wait_open(1)
    topics = [f"T.S{i}" for i in range(2500)]
    h.on_loop(lambda: h.client.unsubscribe(1, topics))
    fake_ws_server.wait_for(lambda s: len(s.connections[0].frames) == 3)
    frames = [json.loads(f) for f in fake_ws_server.frames()]
    assert [f["action"] for f in frames] == ["unsubscribe"] * 3
    assert [len(f["params"].split(",")) for f in frames] == [1000, 1000, 500]
    assert ",".join(f["params"] for f in frames).split(",") == topics


def test_a_wildcard_and_an_explicit_topic_stay_independent(
    make_client: Callable[..., Harness], fake_ws_server: FakeMassiveServer
) -> None:
    h = make_client()
    h.recorder.wait_open(1)
    h.on_loop(lambda: h.client.subscribe(1, ["A.*"]))
    h.on_loop(lambda: h.client.subscribe(1, ["AM.X"]))
    h.on_loop(lambda: h.client.unsubscribe(1, ["A.*"]))
    fake_ws_server.wait_for(lambda s: len(s.connections[0].frames) == 3)
    assert [json.loads(f)["params"] for f in fake_ws_server.frames()] == ["A.*", "AM.X", "A.*"]
    h.recorder.wait_for(lambda e: h.recorder.count("status")(e) == 2)
    assert [s.topics for _, s in h.recorder.of("status")] == [("A.*",), ("AM.X",)]
    fake_ws_server.send(
        json.dumps([{"ev": "A", "sym": "X", "c": 1.5}, {"ev": "AM", "sym": "X", "c": 2.5}])
    )
    h.recorder.wait_for(lambda e: h.recorder.count("data")(e) == 2)
    assert [(d[1], d[2]) for d in h.recorder.of("data")] == [
        ("A", [("X", 1.5)]),
        ("AM", [("X", 2.5)]),
    ]


def test_an_odd_value_reads_as_null_alone_with_the_raw_symbol(
    make_client: Callable[..., Harness],
    fake_ws_server: FakeMassiveServer,
    load_fixture: Callable[[str, str], bytes],
) -> None:
    h = make_client()
    h.recorder.wait_open(1)
    h.on_loop(lambda: h.client.set_required("T", frozenset({"Ticker"}), symbol="Ticker"))
    fake_ws_server.send(load_fixture("massive/ws", "trades_with_bad_row.json").decode())
    h.recorder.wait_for(lambda e: h.recorder.count("data")(e) == 1)
    [(generation, channel, rows, marks, drift)] = h.recorder.of("data")
    assert (generation, channel, marks[RowFault.UNCONVERTIBLE_VALUES].rows) == (1, "T", (1,))
    assert [(r[0], r[1]) for r in rows] == [  # the source's Ticker and Price
        ("AAPL", 114.125),
        ("BRK.A", None),
        ("MSFT", 101.5),
    ]
    assert drift == DriftReport(
        extra=frozenset({"x"}), missing=frozenset({"s"}), unconvertible=frozenset({"p"})
    )
    assert drift.samples["p"] == DriftSample("BRK.A", '"623100.5"')


def test_a_frame_over_one_mebibyte_is_decoded(
    make_client: Callable[..., Harness], fake_ws_server: FakeMassiveServer
) -> None:
    # websockets closes the socket (1009) on a frame above its 1 MiB default; the client
    # lifts that limit, since a burst of options trades easily exceeds it.
    h = make_client()
    h.recorder.wait_open(1)
    rows = [{"ev": "T", "sym": f"S{i:07d}", "p": 1.5, "s": 100, "t": 1} for i in range(40_000)]
    frame = json.dumps(rows)
    assert len(frame) > 2 * 2**20
    fake_ws_server.send(frame)
    h.recorder.wait_for(lambda e: h.recorder.count("data")(e) == 1)
    assert len(h.recorder.of("data")[0][2]) == 40_000
    assert h.recorder.states() == [WsState.CONNECTING]


def test_required_keys_survive_a_reconnect(
    make_client: Callable[..., Harness], fake_ws_server: FakeMassiveServer
) -> None:
    h = make_client()
    h.recorder.wait_open(1)
    h.on_loop(lambda: h.client.set_required("T", frozenset({"SipTimestamp"}), symbol="Ticker"))
    fake_ws_server.close(1000)
    h.recorder.wait_open(2)
    fake_ws_server.send(json.dumps([{"ev": "T", "sym": "AAPL", "p": 1.0}]))
    h.recorder.wait_for(lambda e: h.recorder.count("data")(e) == 1)
    [(generation, _, rows, marks, _)] = h.recorder.of("data")
    mark = marks[RowFault.UNREADABLE_KEYS]
    assert (generation, len(rows), mark.symbols) == (2, 1, ("AAPL",))
    assert "no SipTimestamp (wire key 't'), which the dataset needs" in mark.reasons[0]


def test_a_status_after_authentication_is_delivered_in_order_with_data(
    make_client: Callable[..., Harness], fake_ws_server: FakeMassiveServer
) -> None:
    h = make_client()
    h.recorder.wait_open(1)
    fake_ws_server.send(
        json.dumps(
            [
                {"ev": "status", "status": "max_connections", "message": "too many"},
                {"ev": "T", "sym": "AAPL", "p": 1.0},
            ]
        )
    )
    h.recorder.wait_for(lambda e: h.recorder.count("data")(e) == 1)
    kinds = [e[0] for e in h.recorder.events if e[0] in {"status", "data"}]
    assert kinds == ["status", "data"]
    assert h.recorder.of("status")[0][1].kind is StatusKind.MAX_CONNECTIONS


# --- loop-thread methods ------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("method", "args"),
    [
        ("set_required", ("T", frozenset())),
        ("subscribe", (1, ["T.AAPL"])),
        ("unsubscribe", (1, ["T.AAPL"])),
    ],
)
def test_loop_thread_methods_refuse_other_threads(
    make_client: Callable[..., Harness], method: str, args: tuple[object, ...]
) -> None:
    h = make_client()
    with pytest.raises(RuntimeError, match=rf"WsClient\.{method} must be called on the client"):
        getattr(h.client, method)(*args)


def test_set_required_rejects_an_unknown_column(make_client: Callable[..., Harness]) -> None:
    h = make_client()
    with pytest.raises(ValueError, match=r"has no columns \['Bid'\]"):
        h.on_loop(lambda: h.client.set_required("T", frozenset({"Bid"})))


# --- closing ----------------------------------------------------------------------------------


def test_close_during_a_long_backoff_returns_within_a_second(
    fake_ws_server: FakeMassiveServer,
) -> None:
    recorder = Recorder()
    fake_ws_server.reject_next(503)
    client = WsClient(
        fake_ws_server.url(),
        api_key=KEY,
        sources=SOURCES,
        on_open=recorder.on_open,
        on_status=recorder.on_status,
        on_data=recorder.on_data,
        on_state=recorder.on_state,
        reconnect_backoff=120.0,
        max_reconnect_backoff=120.0,
    )
    client.start()
    client.connect()
    recorder.wait_state(WsState.RECONNECTING)
    began = time.monotonic()
    client.close()
    assert client.join(1.0)
    assert time.monotonic() - began < 1.0
    assert recorder.states()[-1] is WsState.CLOSED


def test_close_closes_an_open_socket_with_1000(
    make_client: Callable[..., Harness], fake_ws_server: FakeMassiveServer
) -> None:
    h = make_client()
    h.recorder.wait_open(1)
    h.client.close()
    assert h.client.join(10.0)
    fake_ws_server.wait_for(lambda s: s.connections[0].close_code is not None)
    assert fake_ws_server.connections[0].close_code == 1000
    assert h.recorder.states() == [WsState.CONNECTING, WsState.CLOSED]


def test_close_from_a_callback_stops_the_client(
    make_client: Callable[..., Harness],
) -> None:
    recorder = Recorder()
    h = make_client(start=False, recorder=recorder)
    recorder.open_hook = lambda _: h.client.close()
    h.client.start()
    h.client.connect()
    assert h.client.join(10.0)
    assert recorder.states() == [WsState.CONNECTING, WsState.CLOSED]


def test_close_before_start_ends_at_once(
    make_client: Callable[..., Harness], fake_ws_server: FakeMassiveServer
) -> None:
    h = make_client(start=False)
    assert h.client.join(0.0)  # never started: nothing to wait for
    h.client.close()
    h.client.start()
    h.client.connect()
    assert h.client.join(10.0)
    assert h.recorder.states() == [WsState.CLOSED]
    assert fake_ws_server.connections == []


def test_a_started_client_opens_no_socket_until_connect(
    make_client: Callable[..., Harness], fake_ws_server: FakeMassiveServer
) -> None:
    # The loop runs from start(): a posted call runs, yet no socket opens until connect().
    h = make_client(start=False)
    h.client.start()
    assert h.on_loop(lambda: "ran") == "ran"
    assert h.recorder.states() == []
    assert fake_ws_server.connections == []
    h.client.connect()
    h.client.connect()  # a second call opens no second socket
    h.recorder.wait_open(1)
    assert h.recorder.states() == [WsState.CONNECTING]
    assert len(fake_ws_server.connections) == 1


def test_connect_before_start_connects_once_the_thread_runs(
    make_client: Callable[..., Harness],
) -> None:
    h = make_client(start=False)
    h.client.connect()
    h.client.start()
    h.recorder.wait_open(1)


def test_close_before_connect_reports_closed_and_opens_no_socket(
    make_client: Callable[..., Harness], fake_ws_server: FakeMassiveServer
) -> None:
    h = make_client(start=False)
    h.client.start()
    assert h.on_loop(lambda: "ran") == "ran"  # the client waits for connect()
    h.client.close()
    assert h.client.join(10.0)
    h.client.connect()  # after the client ended: does nothing
    assert h.recorder.states() == [WsState.CLOSED]
    assert fake_ws_server.connections == []


def test_close_after_the_client_failed_does_nothing(
    make_client: Callable[..., Harness], fake_ws_server: FakeMassiveServer
) -> None:
    fake_ws_server.reject_next(404)
    h = make_client()
    assert h.client.join(10.0)
    h.client.close()
    assert h.recorder.states() == [WsState.CONNECTING, WsState.FAILED]


def test_a_raising_callback_stops_the_client_and_is_logged(
    make_client: Callable[..., Harness],
    fake_ws_server: FakeMassiveServer,
    caplog: pytest.LogCaptureFixture,
) -> None:
    recorder = Recorder()
    h = make_client(start=False, recorder=recorder)

    def boom(_: int) -> None:
        raise ZeroDivisionError

    recorder.open_hook = boom
    h.client.start()
    h.client.connect()
    assert h.client.join(10.0)
    fake_ws_server.wait_for(lambda s: s.connections[0].close_code is not None)
    assert "stopped on an internal error" in caplog.text
    assert recorder.states() == [WsState.CONNECTING, WsState.FAILED]
    error = recorder.of("state")[-1][1]
    assert type(error) is WireError
    # A bug in the client, which the library reports as a defect.
    assert error.kind is WireFailure.CLIENT_DEFECT
    assert isinstance(error.__cause__, ZeroDivisionError)
    assert error.message == (
        "the WebSocket client for 127.0.0.1/stocks stopped on an internal error: "
        "ZeroDivisionError()"
    )


def test_an_unexpected_error_while_connecting_fails_the_client(
    make_client: Callable[..., Harness], monkeypatch: pytest.MonkeyPatch
) -> None:
    # websockets raises ImportError for a SOCKS proxy without python-socks, for example.
    def connect(*args: object, **kwargs: object) -> None:
        raise ImportError("python-socks is required")

    monkeypatch.setattr(client_module, "connect", connect)
    h = make_client()
    assert h.client.join(10.0)
    assert h.recorder.states() == [WsState.CONNECTING, WsState.FAILED]
    assert isinstance(h.recorder.of("state")[-1][1].__cause__, ImportError)


def test_an_on_state_that_raises_at_the_end_is_logged(
    make_client: Callable[..., Harness], caplog: pytest.LogCaptureFixture
) -> None:
    recorder = Recorder()

    def raise_on(state: WsState) -> None:
        raise RuntimeError(state)

    recorder.state_hook = raise_on  # the client gets it through the recorder's on_state
    h = make_client(recorder=recorder)
    assert h.client.join(10.0)
    # CONNECTING raised, which failed the client; FAILED raised too, and was only logged.
    assert recorder.states() == [WsState.CONNECTING, WsState.FAILED]
    assert "on_state raised while the client for" in caplog.text


# --- free-threaded stress -----------------------------------------------------------------------


def test_close_from_16_threads_at_once(make_client: Callable[..., Harness]) -> None:
    h = make_client()
    h.recorder.wait_open(1)
    barrier = threading.Barrier(16)

    def close() -> None:
        barrier.wait()
        h.client.close()

    threads = [threading.Thread(target=close) for _ in range(16)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(10.0)
    assert h.client.join(10.0)
    assert h.recorder.states().count(WsState.CLOSED) == 1


def test_a_second_close_never_interrupts_the_closing_handshake(
    make_client: Callable[..., Harness], fake_ws_server: FakeMassiveServer
) -> None:
    h = make_client()
    h.recorder.wait_open(1)
    fake_ws_server.hold_reads(held=True)  # the client's close frame goes unanswered for now
    h.client.close()
    for _ in range(3):  # each round trip drains the loop: the task is now awaiting the close
        h.on_loop(lambda: None)
    h.client.close()
    for _ in range(3):  # a second cancel, were one sent, has now landed
        h.on_loop(lambda: None)
    fake_ws_server.hold_reads(held=False)
    assert h.client.join(10.0)
    fake_ws_server.wait_for(lambda s: s.connections[0].close_code is not None)
    assert fake_ws_server.connections[0].close_code == 1000


def test_16_clients_at_once(
    make_client: Callable[..., Harness], fake_ws_server: FakeMassiveServer
) -> None:
    harnesses = []
    for i in range(16):
        recorder = Recorder()
        h = make_client(start=False, recorder=recorder)
        recorder.open_hook = lambda g, c=h.client, i=i: c.subscribe(g, [f"T.S{i}"])
        harnesses.append(h)
    for h in harnesses:
        h.client.start()
        h.client.connect()
    for h in harnesses:
        h.recorder.wait_for(lambda e: any(x[0] == "status" for x in e))
    acked = sorted(h.recorder.of("status")[0][1].topics[0] for h in harnesses)
    assert acked == sorted(f"T.S{i}" for i in range(16))
    threads = {h.recorder.threads.pop() for h in harnesses}
    assert len(threads) == 16
