"""Tests for `SessionClients`, the one holder of a session's wire clients."""

import gc
import threading
import weakref
from collections.abc import Callable, Iterator
from typing import Any, cast

import pytest

import deephaven_massive as dm
from deephaven_massive import _massive
from deephaven_massive._datasets.request import HostDecision
from deephaven_massive._enums import ChannelFamily, Recency
from deephaven_massive._ingest import flatfiles, rate_limits, rest, ws
from deephaven_massive._ingest.clients import SessionClients
from deephaven_massive._tables.core import SessionCore
from tests._fakes.s3 import FakeS3


def config(**overrides: object) -> dm.MassiveConfig:
    fields: dict[str, object] = {
        "api_key": "test-key",
        "s3_access_key_id": "AKIDFLATFILES",
        "s3_secret_access_key": "flat-file-secret",
    }
    fields.update(overrides)
    return dm.MassiveConfig(**cast("dict[str, Any]", fields))


def clients_of(m: dm.MassiveSession) -> SessionClients:
    ports = m._core._ports
    assert isinstance(ports, SessionClients)
    return ports


@pytest.fixture
def session(fake_s3: FakeS3) -> Iterator[dm.MassiveSession]:
    m = dm.MassiveSession(config())
    yield m
    m.close()


def test_every_session_gets_its_own_clients(session: dm.MassiveSession) -> None:
    other = dm.MassiveSession(config())
    try:
        assert clients_of(session) is not clients_of(other)
    finally:
        other.close()


def test_no_client_exists_before_the_first_flat_file_read(
    session: dm.MassiveSession, fake_s3: FakeS3
) -> None:
    assert clients_of(session)._flat_files is None
    assert fake_s3.clients == []


def test_every_read_of_a_session_shares_one_client(session: dm.MassiveSession) -> None:
    first = session._core.flat_file_transport()
    second = session._core.flat_file_transport()
    assert isinstance(first, flatfiles.FlatFileHistory)
    assert isinstance(second, flatfiles.FlatFileHistory)
    client = clients_of(session)._flat_files
    assert client is not None
    assert first._client is second._client is client
    assert not client.closed
    session.close()
    assert client.closed  # registered with the session's closers


def test_a_client_created_after_close_is_closed_at_once(fake_s3: FakeS3) -> None:
    m = dm.MassiveSession(config())
    m.close()
    m._core.flat_file_transport()
    client = clients_of(m)._flat_files
    assert client is not None
    assert client.closed


def test_the_flat_file_transport_needs_s3_credentials(fake_s3: FakeS3) -> None:
    m = dm.MassiveSession(config(s3_access_key_id=None, s3_secret_access_key=None))
    try:
        with pytest.raises(ValueError, match="flat files need MassiveConfig s3_access_key_id"):
            m._core.flat_file_transport()
    finally:
        m.close()
    assert clients_of(m)._flat_files is None


def test_every_stream_transport_shares_the_sessions_connections(
    session: dm.MassiveSession,
) -> None:
    first = session._core.stream_transport()
    second = session._core.stream_transport()
    assert isinstance(first, ws.WsTransport)
    assert isinstance(second, ws.WsTransport)
    assert first is not second
    assert first._connections is second._connections is clients_of(session)._streams
    assert first._core is session._core  # the transport holds the core for its verb


def test_the_connections_close_with_the_session(fake_s3: FakeS3) -> None:
    m = dm.MassiveSession(config())
    m._core.stream_transport()
    connections = clients_of(m)._streams
    assert connections is not None
    m.close()
    assert connections._closed


class _HostCache:
    # The two host-decision calls the WebSocket side makes of the REST transport.
    def __init__(self) -> None:
        self.calls: list[tuple[object, ...]] = []

    def resolve_host(self, asset_class: dm.AssetClass, family: ChannelFamily) -> HostDecision:
        self.calls.append(("resolve", asset_class, family))
        return HostDecision("socket.massive.com", Recency.REALTIME, "test")

    def record_verdict(
        self, asset_class: dm.AssetClass, family: ChannelFamily, decision: HostDecision
    ) -> None:
        self.calls.append(("record", asset_class, family, decision))


def test_host_decisions_go_through_the_rest_transport(
    session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    cache = _HostCache()
    monkeypatch.setattr(SessionClients, "rest", lambda _: cache)
    clients = clients_of(session)
    decision = clients._resolve_host(dm.AssetClass.STOCKS, ChannelFamily.TICK)
    verdict = HostDecision("delayed.massive.com", Recency.DELAYED, "server verdict: x")
    clients._record_verdict(dm.AssetClass.FUTURES, ChannelFamily.TICK, verdict)
    assert decision.host == "socket.massive.com"
    assert cache.calls == [
        ("resolve", dm.AssetClass.STOCKS, ChannelFamily.TICK),
        ("record", dm.AssetClass.FUTURES, ChannelFamily.TICK, verdict),
    ]


def test_the_connections_do_not_keep_the_core_alive(fake_s3: FakeS3) -> None:
    # The connections live as long as the session and their close is a session closer, which the
    # session's finalizer holds: a strong reference to the core would keep it alive forever.
    m = dm.MassiveSession(config())
    m._core.stream_transport()
    connections = clients_of(m)._streams
    core = weakref.ref(m._core)
    gc.disable()
    try:
        del m
        assert core() is None  # freed by reference counting alone
    finally:
        gc.enable()
    assert connections is not None
    closed = threading.Event()
    for _ in range(200):  # its closer runs on the reaper
        if connections._closed:
            break
        closed.wait(0.05)
    assert connections._closed


def test_every_rest_call_of_a_session_shares_one_client(session: dm.MassiveSession) -> None:
    assert clients_of(session)._rest is None  # nothing is created before the first REST call
    first = session._core.rest_transport()
    second = session._core.rest_transport()
    assert isinstance(first, rest.RestEndpoints)
    assert isinstance(second, rest.RestEndpoints)
    state = clients_of(session)._rest
    assert state is not None
    assert first._state is second._state is state
    assert first._core is session._core  # a transport holds the core itself
    session.close()
    assert state.closed  # registered with the session's closers


def test_a_rest_client_created_after_close_is_closed_at_once(fake_s3: FakeS3) -> None:
    m = dm.MassiveSession(config())
    m.close()
    m._core.rest_transport()
    state = clients_of(m)._rest
    assert state is not None
    assert state.closed


def test_racing_first_rest_calls_keep_one_client(
    session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The client is built outside the lock; a call that loses the race closes its own.
    built: list[_massive.RestClient] = []
    real = _massive.RestClient

    def factory(api_key: str, **settings: object) -> _massive.RestClient:
        client = real(api_key, **cast("dict[str, Any]", settings))
        built.append(client)
        if len(built) == 1:  # the other call installs its client first
            session._core.rest_transport()
        return client

    monkeypatch.setitem(vars(_massive), "RestClient", factory)
    transport = session._core.rest_transport()
    assert isinstance(transport, rest.RestEndpoints)
    assert len(built) == 2
    assert transport._state.client is built[1]
    with pytest.raises(_massive.WireError, match="closed"):
        built[0].get(
            _massive.source(_massive.Transport.REST, "/v3/snapshot"), {}, {}, bucket="other"
        )


def test_the_rest_state_does_not_keep_the_core_alive(fake_s3: FakeS3) -> None:
    m = dm.MassiveSession(config())
    m._core.rest_transport()
    core = weakref.ref(m._core)
    state = clients_of(m)._rest
    assert state is not None
    gc.disable()
    try:
        del m
        assert core() is None  # freed by reference counting alone
    finally:
        gc.enable()
    # The collected session's closer runs on its reaper; it closes the client.
    assert _wait_for(lambda: state.closed)


def _wait_for(condition: Callable[[], bool]) -> bool:
    deadline = threading.Event()
    for _ in range(1000):
        if condition():
            return True
        deadline.wait(0.01)
    return condition()


def test_the_clients_do_not_keep_the_core_alive(fake_s3: FakeS3) -> None:
    # The core holds its clients, so a strong reference back would make a cycle, and an unclosed
    # session would then wait for the cyclic collector before its connections are closed.
    m = dm.MassiveSession(config())
    m._core.flat_file_transport()
    core = weakref.ref(m._core)
    gc.disable()
    try:
        del m
        assert core() is None  # freed by reference counting alone
    finally:
        gc.enable()


def test_a_transport_holds_the_core_itself(session: dm.MassiveSession) -> None:
    # A transport outlives the call that made it, so it must never hold a weak reference.
    transport = session._core.flat_file_transport()
    assert isinstance(transport, flatfiles.FlatFileHistory)
    assert type(transport._core) is SessionCore
    assert transport._core is session._core


def test_clients_whose_session_is_gone_report_it_closed(fake_s3: FakeS3) -> None:
    m = dm.MassiveSession(config())
    clients = clients_of(m)
    label = m._core.label
    m.close()
    del m
    gc.collect()
    with pytest.raises(dm.MassiveError, match=rf"^{label} is closed$"):
        clients.flat_files()


def test_the_client_is_created_under_the_registry_lock(
    session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Two racing first reads must build one client: the creation holds `registry_lock`, which
    # another thread then cannot take.
    lock = session._core.registry_lock
    taken_elsewhere: list[bool] = []

    def try_lock() -> None:
        acquired = lock.acquire(blocking=False)
        taken_elsewhere.append(acquired)
        if acquired:
            lock.release()

    real = _massive.FlatFileClient

    def factory(**settings: object) -> _massive.FlatFileClient:
        probe = threading.Thread(target=try_lock)
        probe.start()
        probe.join()
        return real(**cast("dict[str, Any]", settings))

    # `setitem`, as the flat-file tests do: teardown removes the entry again.
    monkeypatch.setitem(vars(_massive), "FlatFileClient", factory)
    session._core.flat_file_transport()
    session._core.flat_file_transport()
    assert taken_elsewhere == [False]


def test_sessions_of_one_key_share_a_governor_and_its_declarations(fake_s3: FakeS3) -> None:
    first = dm.MassiveSession(config(rest_basic_subscriptions={dm.Subscription.OPTIONS}))
    second = dm.MassiveSession(config())
    other_key = dm.MassiveSession(config(api_key="other-key"))
    try:
        shared = clients_of(first)._lease.governor
        assert clients_of(second)._lease.governor is shared
        assert clients_of(other_key)._lease.governor is not shared
        second._core.rest_transport()
        rest_state = clients_of(second)._rest
        assert rest_state is not None
        assert rest_state.client.governor is shared  # the session's client uses it
        assert {s.bucket for s in shared.snapshot() if s.declared} == {"options"}
        first.close()  # the declaration leaves with its session
        assert {s.bucket for s in shared.snapshot() if s.declared} == set()
    finally:
        for m in (first, second, other_key):
            m.close()
    entries = rate_limits.GOVERNORS._entries.values()
    [entry] = [each for each in entries if each.governor is shared]
    assert entry.holders == {}


def test_a_collected_session_releases_its_lease(fake_s3: FakeS3) -> None:
    m = dm.MassiveSession(config(rest_basic_subscriptions={dm.Subscription.STOCKS}))
    governor = clients_of(m)._lease.governor
    gc.disable()
    try:
        del m
    finally:
        gc.enable()
    # The collected session's closers run on its reaper; its declaration goes with it.
    assert _wait_for(lambda: not any(s.declared for s in governor.snapshot()))
