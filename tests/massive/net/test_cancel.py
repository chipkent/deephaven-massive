"""Tests for `Cancel`, the token that ends a request's waits."""

from __future__ import annotations

import threading

import pytest

from deephaven_massive._massive.net.cancel import Cancel


def test_a_new_token_is_not_set_and_is_an_event() -> None:
    token = Cancel()
    assert isinstance(token, threading.Event)
    assert not token.is_set()
    assert not token.wait(0)


def test_set_wakes_waiters_and_runs_callbacks_once_in_order() -> None:
    token = Cancel()
    calls: list[str] = []
    token.on_set(lambda: calls.append("a"))
    token.on_set(lambda: calls.append("b"))
    token.set()
    token.set()
    assert token.is_set()
    assert token.wait(0)
    assert calls == ["a", "b"]


def test_a_callback_registered_after_set_runs_at_once() -> None:
    token = Cancel()
    token.set()
    calls: list[int] = []
    remove = token.on_set(lambda: calls.append(1))
    assert calls == [1]
    remove()  # does nothing, any number of times
    remove()


def test_a_removed_callback_never_runs() -> None:
    token = Cancel()
    calls: list[int] = []
    remove = token.on_set(lambda: calls.append(1))
    remove()
    remove()
    token.set()
    assert calls == []


def test_a_token_cannot_be_cleared() -> None:
    token = Cancel()
    token.set()
    with pytest.raises(RuntimeError, match="cannot be cleared"):
        token.clear()
    assert token.is_set()


def test_a_child_follows_any_parent_and_never_sets_its_parents() -> None:
    closed, stop = Cancel(), Cancel()
    child = Cancel(closed, stop)
    stop.set()
    assert child.is_set()
    assert not closed.is_set()
    other = Cancel(closed, stop)  # born set: a parent already is
    assert other.is_set()
    lone = Cancel(closed)
    lone.set()
    assert not closed.is_set()


def test_a_closed_child_no_longer_follows_and_is_released_by_its_parents() -> None:
    parent = Cancel()
    with Cancel(parent) as child:
        assert parent._callbacks
    assert parent._callbacks == {}  # the parent keeps no reference to the child
    child.close()  # idempotent
    parent.set()
    assert not child.is_set()


def test_set_races_on_set_without_losing_a_callback() -> None:
    # Whichever comes first, every callback registered runs exactly once.
    for _ in range(200):
        token = Cancel()
        calls: list[int] = []
        lock = threading.Lock()

        def note(lock: threading.Lock = lock, calls: list[int] = calls) -> None:
            with lock:
                calls.append(1)

        threads = [threading.Thread(target=token.on_set, args=(note,)) for _ in range(4)]
        setter = threading.Thread(target=token.set)
        for thread in [*threads[:2], setter, *threads[2:]]:
            thread.start()
        for thread in [*threads, setter]:
            thread.join(10)
        assert len(calls) == 4
