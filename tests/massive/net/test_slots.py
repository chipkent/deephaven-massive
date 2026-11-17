"""Tests for `ConnectionSlots`, the bound on a client's requests in flight."""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from deephaven_massive._massive.net.cancel import Cancel
from deephaven_massive._massive.net.slots import ConnectionSlots

_TIMEOUT = 10.0


def _watched(slots: ConnectionSlots) -> threading.Event:
    # Set as soon as a thread waits for a slot, so a test sees it blocked without a sleep.
    waiting = threading.Event()
    wait = slots._ready.wait

    def noted(timeout: float | None = None) -> bool:
        waiting.set()
        return wait(timeout)

    slots._ready.wait = noted  # type: ignore[method-assign]  # the point: observe the wait
    return waiting


def test_slots_are_taken_until_none_is_free_and_given_back() -> None:
    slots = ConnectionSlots(2)
    stop = Cancel()
    assert slots.acquire(stop)
    assert slots.acquire(stop)
    stop.set()  # every slot is taken: a stopped caller gets none and does not wait
    assert not slots.acquire(stop)
    slots.release()
    assert slots.acquire(Cancel())


def test_a_set_token_takes_no_slot_even_when_one_is_free() -> None:
    slots = ConnectionSlots(1)
    stop = Cancel()
    stop.set()
    assert not slots.acquire(stop)
    assert slots.acquire(Cancel())  # the free slot is still there


def test_a_waiting_caller_gets_the_slot_given_back() -> None:
    slots = ConnectionSlots(1)
    assert slots.acquire(Cancel())
    waiting = _watched(slots)
    with ThreadPoolExecutor(1) as pool:
        taken = pool.submit(slots.acquire, Cancel())
        assert waiting.wait(_TIMEOUT)
        assert not taken.done()
        slots.release()
        assert taken.result(_TIMEOUT) is True


def test_setting_the_token_ends_the_wait_without_a_slot() -> None:
    slots = ConnectionSlots(1)
    assert slots.acquire(Cancel())
    waiting = _watched(slots)
    stop = Cancel()
    with ThreadPoolExecutor(1) as pool:
        taken = pool.submit(slots.acquire, stop)
        assert waiting.wait(_TIMEOUT)
        stop.set()
        assert taken.result(_TIMEOUT) is False
    slots.release()  # the one slot taken above; the stopped caller took none
    with pytest.raises(
        RuntimeError, match=r"^A connection slot was given back that was never taken\.$"
    ):
        slots.release()


def test_a_slot_given_back_reaches_a_waiter_whose_neighbour_was_stopped() -> None:
    # Two callers wait; one is stopped and leaves. The slot given back must still reach the
    # other, however the wake-ups are ordered.
    slots = ConnectionSlots(1)
    assert slots.acquire(Cancel())
    stopped, patient = Cancel(), Cancel()
    with ThreadPoolExecutor(2) as pool:
        first = pool.submit(slots.acquire, stopped)
        second = pool.submit(slots.acquire, patient)
        stopped.set()
        assert first.result(_TIMEOUT) is False
        slots.release()
        assert second.result(_TIMEOUT) is True


def test_no_more_callers_than_slots_hold_one_at_once() -> None:
    # Many threads take and give back slots; the number held never exceeds the size.
    slots = ConnectionSlots(3)
    lock = threading.Lock()
    held = most = 0

    def work() -> None:
        nonlocal held, most
        for _ in range(200):
            assert slots.acquire(Cancel())
            with lock:
                held += 1
                most = max(most, held)
            with lock:
                held -= 1
            slots.release()

    with ThreadPoolExecutor(12) as pool:
        for future in [pool.submit(work) for _ in range(12)]:
            future.result(_TIMEOUT)
    assert 1 <= most <= 3
    assert all(slots.acquire(Cancel()) for _ in range(3))  # every slot came back


def test_a_client_needs_a_slot() -> None:
    with pytest.raises(ValueError, match=r"^size=0 is not at least 1 connection slot\.$"):
        ConnectionSlots(0)
