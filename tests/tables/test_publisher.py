"""Tests for the blink-table publisher whose failure is delivered exactly once, in the embedded JVM.

Each test creates a `blink_publisher` table in its own scope and records what its shutdown
callback saw and the scopes the publisher handed to `release_later` (in a session, the reaper
releases them), then drives the update graph through its shared lock: while the lock is held,
no update cycle runs, so a published failure stays queued.
"""

from __future__ import annotations

import logging
import threading
import types
from typing import TYPE_CHECKING

import pytest
from deephaven import dtypes
from deephaven.update_graph import shared_lock

from deephaven_massive._errors import MassiveError
from deephaven_massive._tables.liveness import new_scope, release_scope
from deephaven_massive._tables.publisher import _FailurePin, blink_publisher

if TYPE_CHECKING:
    from deephaven.execution_context import ExecutionContext
    from deephaven.liveness_scope import LivenessScope

TIMEOUT = 15.0


class _Pinned:
    # A `blink_publisher` table in its own scope, with what its shutdown callback saw and the
    # scopes the publisher handed on for release.
    def __init__(self, exec_ctx: ExecutionContext) -> None:
        self._exec_ctx = exec_ctx
        self._update_graph = exec_ctx.update_graph.j_update_graph
        self.shut_down = threading.Event()
        # The shutdown callback and `release_later` may run on an update-graph thread, so the
        # lists they fill are read and written under this lock.
        self._lock = threading.Lock()
        self._on_update_graph: list[bool] = []
        self._deferred: list[LivenessScope] = []
        self._deferred_at_shutdown: list[int] = []
        self.scope = new_scope()
        with self.scope.open():
            self.blink, self.publisher = blink_publisher(
                "test.pinned",
                {"X": dtypes.int64},
                on_shutdown=self._on_shutdown,
                release_later=self._hand_on,
            )
        self.published: list[str] = []

    def _on_shutdown(self) -> None:
        on_update_graph = bool(self._update_graph.currentThreadProcessesUpdates())
        with self._lock:
            self._deferred_at_shutdown.append(len(self._deferred))
            self._on_update_graph.append(on_update_graph)
        self.shut_down.set()

    def release_deferred(self) -> None:
        # What the session's reaper does with each scope handed to `release_later`.
        with self._lock:
            scopes, self._deferred = self._deferred, []
        for scope in scopes:
            release_scope(self._exec_ctx, scope)

    def _hand_on(self, scope: LivenessScope) -> None:
        with self._lock:
            self._deferred.append(scope)

    def deferred(self) -> int:
        # How many scopes were handed on and not yet released.
        with self._lock:
            return len(self._deferred)

    def deferred_at_shutdown(self) -> list[int]:
        # How many scopes had been handed on when each shutdown callback ran.
        with self._lock:
            return list(self._deferred_at_shutdown)

    def on_update_graph(self) -> list[bool]:
        # Whether each shutdown callback ran on an update-graph thread.
        with self._lock:
            return list(self._on_update_graph)

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
    dh_exec_ctx: ExecutionContext, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG, logger="deephaven_massive._tables.publisher")
    # Releasing the last scope of a table whose failure is queued must not destroy it. A
    # destroy on a thread that does not hold the update graph's lock (Deephaven releases a
    # closed panel's table on a gRPC thread) races the failure's delivery inside Deephaven,
    # which can then deliver the failure twice: a fatal assertion that exits the process.
    pinned = _Pinned(dh_exec_ctx)
    with shared_lock(dh_exec_ctx.update_graph):
        pinned.publisher.publish_failure(MassiveError("refused"))
        pinned.scope.release()
        early = pinned.shut_down.is_set()
    assert not early  # pinned: the release did not destroy the table
    assert pinned.shut_down.wait(TIMEOUT)
    # The delivery shut the table down, inside the update cycle that failed it, and the pin
    # was handed on before the owner's callback ran.
    assert pinned.on_update_graph() == [True]
    assert pinned.deferred_at_shutdown() == [1]
    assert pinned.failed()
    assert not pinned.destroyed()
    assert "test.pinned: its failure pin is handed to the reaper for release" in caplog.messages
    pinned.release_deferred()  # the session's reaper releases the pin, and the table dies
    assert pinned.destroyed()
    assert pinned.on_update_graph() == [True]  # the adapter had already shut down


def test_a_destroyed_table_publishes_no_failure(
    dh_exec_ctx: ExecutionContext,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG, logger="deephaven_massive._tables.publisher")
    pinned = _Pinned(dh_exec_ctx)
    release_scope(dh_exec_ctx, pinned.scope)
    assert pinned.on_update_graph() == [False]  # the release destroyed it, on this thread
    assert pinned.deferred_at_shutdown() == [0]  # no failure, no pin
    pinned.record_publishes(monkeypatch)
    pinned.publisher.publish_failure(MassiveError("too late"))
    assert pinned.deferred() == 1  # the refused failure's scope, which manages nothing
    assert (
        "test.pinned: failure not published, the table has shut down or an earlier failure is "
        "still pinned (too late); its unused scope is handed to the reaper for release"
    ) in caplog.messages
    pinned.release_deferred()
    # A destroy on another thread that has not yet reached the shutdown callback: the pin does
    # not know the table shut down, and only the failed `tryManage` keeps the failure out. The
    # pin keeps its empty scope for the shutdown callback the destroy is about to run.
    monkeypatch.setattr(pinned.publisher, "_pin", _FailurePin("test.pinned"))
    pinned.publisher.publish_failure(MassiveError("too late, mid-destroy"))
    assert pinned.published == []
    assert pinned.deferred() == 0
    assert (
        "test.pinned: failure not published, the table is being destroyed (too late, mid-destroy)"
        in caplog.messages
    )


def test_a_shutdown_right_after_the_pin_is_reserved_publishes_nothing(
    dh_exec_ctx: ExecutionContext, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The table is destroyed on another thread just after `publish_failure` reserved its pin:
    # the shutdown callback hands the still-empty scope on, and the reaper releases it before
    # the publisher makes it manage the table. The failure must be declined, not raise.
    pinned = _Pinned(dh_exec_ctx)
    pinned.record_publishes(monkeypatch)
    hold = _FailurePin.hold

    def hold_then_shut_down(pin: _FailurePin, scope: LivenessScope) -> bool:
        held = hold(pin, scope)
        release_scope(dh_exec_ctx, pinned.scope)  # the destroy runs the shutdown callback
        pinned.release_deferred()  # the reaper releases the reserved scope
        return held

    monkeypatch.setattr(_FailurePin, "hold", hold_then_shut_down)
    pinned.publisher.publish_failure(MassiveError("raced"))
    assert pinned.published == []
    assert pinned.deferred_at_shutdown() == [1]  # the reserved scope went to the reaper
    assert pinned.destroyed()


def test_a_second_failure_while_the_first_is_queued_is_not_published(
    dh_exec_ctx: ExecutionContext, monkeypatch: pytest.MonkeyPatch
) -> None:
    pinned = _Pinned(dh_exec_ctx)
    pinned.record_publishes(monkeypatch)
    with shared_lock(dh_exec_ctx.update_graph):
        pinned.publisher.publish_failure(MassiveError("first"))
        pinned.publisher.publish_failure(MassiveError("second"))
        refused = pinned.deferred()
    assert pinned.published == ["first"]
    assert refused == 1  # the second failure's scope, never pinned, handed on at once
    assert pinned.shut_down.wait(TIMEOUT)
    assert pinned.deferred_at_shutdown() == [2]  # then the first failure's pin
    pinned.release_deferred()
    release_scope(dh_exec_ctx, pinned.scope)
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
    pinned.release_deferred()
    assert not pinned.destroyed()
    pinned.publisher.publish_failure(MassiveError("again"))
    assert pinned.published == ["first"]
    assert pinned.deferred() == 1  # the refused failure's scope, which manages nothing
    release_scope(dh_exec_ctx, pinned.scope)
    assert pinned.destroyed()  # the refused scope holds nothing: it was refused before it pinned
    pinned.release_deferred()


def test_a_failure_that_raises_unpins_its_table(
    dh_exec_ctx: ExecutionContext, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The failure was not queued, so nothing would ever deliver it and release the pin. The
    # pin is handed on for release at once and the table is not marked shut down, so the
    # failure can be retried right away, before the reaper has released the first pin.
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
    assert pinned.deferred() == 1  # the interrupted failure's pin
    monkeypatch.setattr(pinned.publisher, "_publisher", real)
    pinned.record_publishes(monkeypatch)
    with shared_lock(dh_exec_ctx.update_graph):
        pinned.publisher.publish_failure(MassiveError("refused, again"))
        pinned.release_deferred()  # the first pin's release lands while the retry is queued
        pinned.scope.release()
        early = pinned.shut_down.is_set()
    assert pinned.published == ["refused, again"]
    assert not early  # the retry pinned the table: the releases did not destroy it
    assert pinned.shut_down.wait(TIMEOUT)
    assert pinned.failed()
    assert pinned.deferred_at_shutdown() == [1]
    pinned.release_deferred()
    assert pinned.destroyed()
