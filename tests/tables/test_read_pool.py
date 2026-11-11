"""Tests for the session's read pool: bounded daemon threads, reuse, stop, and errors."""

from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING

from deephaven_massive._tables.read_pool import ReadPool

if TYPE_CHECKING:
    from collections.abc import Callable

    import pytest

TIMEOUT = 10.0


def _task(started: threading.Event, release: threading.Event) -> Callable[[], None]:
    # A task that says it started, then waits until the test releases it.
    def run() -> None:
        started.set()
        assert release.wait(TIMEOUT)

    return run


def test_no_thread_starts_before_the_first_task() -> None:
    pool = ReadPool("massive #1", 2)
    assert pool.threads == 0
    pool.stop()


def test_tasks_beyond_the_limit_wait_for_a_free_thread() -> None:
    pool = ReadPool("massive #1", 2)
    release = threading.Event()
    started = [threading.Event() for _ in range(3)]
    for event in started:
        assert pool.submit(_task(event, release))
    assert started[0].wait(TIMEOUT)
    assert started[1].wait(TIMEOUT)
    assert not started[2].is_set()  # both threads are busy: the third waits its turn
    assert pool.threads == 2
    release.set()
    assert started[2].wait(TIMEOUT)
    assert pool.threads == 2
    pool.stop()


def test_an_idle_thread_takes_the_next_task_without_a_new_thread() -> None:
    pool = ReadPool("massive #1", 4)
    names: list[str] = []
    for _ in range(3):
        done = threading.Event()

        def run(done: threading.Event = done) -> None:
            names.append(threading.current_thread().name)
            done.set()

        assert pool.submit(run)
        assert done.wait(TIMEOUT)
    assert pool.threads == 1
    assert names == ["massive #1 read_0"] * 3
    pool.stop()


def test_the_threads_are_daemons_so_a_download_never_holds_python_at_exit() -> None:
    pool = ReadPool("massive #1", 1)
    daemon: list[bool] = []
    done = threading.Event()

    def run() -> None:
        daemon.append(threading.current_thread().daemon)
        done.set()

    pool.submit(run)
    assert done.wait(TIMEOUT)
    assert daemon == [True]
    pool.stop()


def test_stop_drops_the_waiting_tasks_and_refuses_new_ones(
    caplog: pytest.LogCaptureFixture,
) -> None:
    pool = ReadPool("massive #9", 1)
    started, release = threading.Event(), threading.Event()
    pool.submit(_task(started, release))
    assert started.wait(TIMEOUT)
    ran: list[str] = []
    pool.submit(lambda: ran.append("waiting"))
    with caplog.at_level(logging.DEBUG, logger="deephaven_massive._tables.read_pool"):
        pool.stop()
        pool.stop()  # idempotent
    assert caplog.messages == [
        "massive #9 read pool: stopped, 1 threads end, 1 waiting tasks dropped"
    ]
    assert not pool.submit(lambda: ran.append("late"))
    release.set()  # the running task finishes; its thread then ends
    thread = next(t for t in threading.enumerate() if t.name == "massive #9 read_0")
    thread.join(TIMEOUT)
    assert not thread.is_alive()
    assert ran == []


def test_a_task_that_raises_is_logged_and_the_thread_carries_on(
    caplog: pytest.LogCaptureFixture,
) -> None:
    pool = ReadPool("massive #1", 1)
    done = threading.Event()

    def broken() -> None:
        raise RuntimeError("boom")

    with caplog.at_level(logging.ERROR, logger="deephaven_massive._tables.read_pool"):
        pool.submit(broken)
        pool.submit(done.set)
        assert done.wait(TIMEOUT)
    [record] = caplog.records
    assert record.getMessage().startswith("massive #1 read pool: a task failed.")
    assert record.exc_info is not None
    assert pool.threads == 1
    pool.stop()
