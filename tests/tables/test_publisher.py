"""Tests for the blink-table publisher whose failure is delivered exactly once, in the embedded JVM.

Each test creates a `blink_publisher` table in its own scope and records what its shutdown
callback saw and the pin releases it deferred, then drives the update graph through its shared
lock: while the lock is held, no update cycle runs, so a published failure stays queued.
"""

from __future__ import annotations

import threading
import types
from typing import TYPE_CHECKING

import pytest
from deephaven import dtypes
from deephaven.update_graph import shared_lock

from deephaven_massive._errors import MassiveError
from deephaven_massive._tables.liveness import new_scope
from deephaven_massive._tables.publisher import _FailurePin, blink_publisher

if TYPE_CHECKING:
    from collections.abc import Callable

    from deephaven.execution_context import ExecutionContext

TIMEOUT = 15.0


class _Pinned:
    # A `blink_publisher` table in its own scope, with what its shutdown callback saw and the
    # pin releases it deferred.
    def __init__(self, exec_ctx: ExecutionContext) -> None:
        self._update_graph = exec_ctx.update_graph.j_update_graph
        self.shut_down = threading.Event()
        self.on_update_graph: list[bool] = []
        self.deferred: list[Callable[[], None]] = []
        self.deferred_at_shutdown: list[int] = []
        self.scope = new_scope()
        with self.scope.open():
            self.blink, self.publisher = blink_publisher(
                "test.pinned",
                {"X": dtypes.int64},
                on_shutdown=self._on_shutdown,
                defer=self.deferred.append,
            )
        self.published: list[str] = []

    def _on_shutdown(self) -> None:
        self.deferred_at_shutdown.append(len(self.deferred))
        self.on_update_graph.append(bool(self._update_graph.currentThreadProcessesUpdates()))
        self.shut_down.set()

    def record_publishes(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Records each failure that reaches Deephaven's publisher, and still publishes it.
        real = self.publisher._publisher

        def publish(error: Exception) -> None:
            self.published.append(str(error))
            real.publish_failure(error)

        recording = types.SimpleNamespace(publish_failure=publish)
        monkeypatch.setattr(self.publisher, "_publisher", recording)

    def failed(self) -> bool:
        return bool(self.blink.j_table.isFailed())

    def destroyed(self) -> bool:
        retained = bool(self.blink.j_table.tryRetainReference())
        if retained:
            self.blink.j_table.dropReference()
        return not retained


def test_a_queued_failure_keeps_its_table_alive_until_it_is_delivered(
    dh_exec_ctx: ExecutionContext,
) -> None:
    # Releasing the last scope of a table whose failure is queued must not destroy it. A
    # destroy on another thread races the failure's delivery inside Deephaven, which can then
    # deliver the failure twice: a fatal assertion that exits the process.
    pinned = _Pinned(dh_exec_ctx)
    with shared_lock(dh_exec_ctx.update_graph):
        pinned.publisher.publish_failure(MassiveError("refused"))
        pinned.scope.release()
        early = pinned.shut_down.is_set()
    assert not early  # pinned: the release did not destroy the table
    assert pinned.shut_down.wait(TIMEOUT)
    # The delivery shut the table down, inside the update cycle that failed it, and the pin's
    # release was queued before the owner's callback ran.
    assert pinned.on_update_graph == [True]
    assert pinned.deferred_at_shutdown == [1]
    assert pinned.failed()
    assert not pinned.destroyed()
    assert len(pinned.deferred) == 1
    pinned.deferred[0]()  # the session's reaper releases the pin, and the table dies
    assert pinned.destroyed()
    assert pinned.on_update_graph == [True]  # the adapter had already shut down


def test_a_destroyed_table_publishes_no_failure(
    dh_exec_ctx: ExecutionContext, monkeypatch: pytest.MonkeyPatch
) -> None:
    pinned = _Pinned(dh_exec_ctx)
    pinned.scope.release()
    assert pinned.on_update_graph == [False]  # the release destroyed it, on this thread
    assert pinned.deferred_at_shutdown == [0]  # no failure, no pin
    pinned.record_publishes(monkeypatch)
    pinned.publisher.publish_failure(MassiveError("too late"))
    # A destroy on another thread that has not yet reached the shutdown callback: the pin does
    # not know the table shut down, and only the failed `tryManage` keeps the failure out.
    monkeypatch.setattr(pinned.publisher, "_pin", _FailurePin())
    pinned.publisher.publish_failure(MassiveError("too late, mid-destroy"))
    assert pinned.published == []
    assert pinned.deferred == []


def test_a_second_failure_while_the_first_is_queued_is_not_published(
    dh_exec_ctx: ExecutionContext, monkeypatch: pytest.MonkeyPatch
) -> None:
    pinned = _Pinned(dh_exec_ctx)
    pinned.record_publishes(monkeypatch)
    with shared_lock(dh_exec_ctx.update_graph):
        pinned.publisher.publish_failure(MassiveError("first"))
        pinned.publisher.publish_failure(MassiveError("second"))
    assert pinned.published == ["first"]
    assert pinned.shut_down.wait(TIMEOUT)
    assert len(pinned.deferred) == 1  # one pin: the second one was released at once
    pinned.deferred[0]()
    pinned.scope.release()
    assert pinned.destroyed()  # no pin is left behind


def test_a_failure_after_the_table_shut_down_is_not_published(
    dh_exec_ctx: ExecutionContext, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The scope still holds the table after its failure was delivered and its pin released,
    # so a new pin would succeed; the shut-down table refuses it.
    pinned = _Pinned(dh_exec_ctx)
    pinned.record_publishes(monkeypatch)
    pinned.publisher.publish_failure(MassiveError("first"))
    assert pinned.shut_down.wait(TIMEOUT)
    pinned.deferred[0]()
    assert not pinned.destroyed()
    pinned.publisher.publish_failure(MassiveError("again"))
    assert pinned.published == ["first"]
    assert len(pinned.deferred) == 1
    pinned.scope.release()
    assert pinned.destroyed()  # the refused pin was released at once


def test_a_failure_that_raises_unpins_its_table(
    dh_exec_ctx: ExecutionContext, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The failure was not queued, so nothing would ever deliver it and release the pin. The
    # pin is released and the table is not marked shut down, so the failure can be retried.
    pinned = _Pinned(dh_exec_ctx)
    real = pinned.publisher._publisher

    def interrupted(_error: Exception) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(
        pinned.publisher, "_publisher", types.SimpleNamespace(publish_failure=interrupted)
    )
    with pytest.raises(KeyboardInterrupt):
        pinned.publisher.publish_failure(MassiveError("refused"))
    assert not pinned.failed()
    monkeypatch.setattr(pinned.publisher, "_publisher", real)
    pinned.record_publishes(monkeypatch)
    with shared_lock(dh_exec_ctx.update_graph):
        pinned.publisher.publish_failure(MassiveError("refused, again"))
        pinned.scope.release()
        early = pinned.shut_down.is_set()
    assert pinned.published == ["refused, again"]
    assert not early  # the retry pinned the table: the release did not destroy it
    assert pinned.shut_down.wait(TIMEOUT)
    assert pinned.failed()
    assert pinned.deferred_at_shutdown == [1]
    pinned.deferred[0]()
    assert pinned.destroyed()
