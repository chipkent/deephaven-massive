"""Tests for `SessionClients`, the one holder of a session's wire clients."""

import gc
import weakref
from collections.abc import Iterator
from typing import Any, cast

import pytest

import deephaven_massive as dm
from deephaven_massive._ingest import flatfiles
from deephaven_massive._ingest.clients import SessionClients
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


def test_the_rest_and_websocket_transports_are_not_implemented_yet(
    session: dm.MassiveSession,
) -> None:
    with pytest.raises(NotImplementedError, match="the REST transport is not implemented yet"):
        session._core.rest_transport()
    with pytest.raises(NotImplementedError, match="the WebSocket transport is not implemented"):
        session._core.stream_transport()


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
