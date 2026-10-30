import logging
import threading

import pytest

from deephaven_massive import _log_limits
from deephaven_massive._log_limits import BoundedLog, log_once


def test_log_once_logs_each_key_once(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(_log_limits, "_logged", set())
    log = logging.getLogger("test.log_once")
    with caplog.at_level(logging.DEBUG, logger="test.log_once"):
        log_once(log, "a", "first %s", 1)
        log_once(log, "a", "again %s", 2)
        log_once(log, "b", "other", level=logging.WARNING)
    assert [(r.levelno, r.getMessage()) for r in caplog.records] == [
        (logging.INFO, "first 1"),
        (logging.WARNING, "other"),
    ]


def test_log_once_is_once_across_threads(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(_log_limits, "_logged", set())
    log = logging.getLogger("test.log_once.threads")
    start = threading.Barrier(16)

    def worker() -> None:
        start.wait(10.0)
        log_once(log, "shared", "once")

    with caplog.at_level(logging.INFO, logger="test.log_once.threads"):
        threads = [threading.Thread(target=worker) for _ in range(16)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(10.0)
    assert caplog.messages == ["once"]


@pytest.mark.parametrize("level", [logging.ERROR, logging.WARNING, logging.DEBUG])
def test_bounded_log_reports_each_value_once_up_to_the_limit(
    caplog: pytest.LogCaptureFixture, level: int
) -> None:
    bad = BoundedLog(logging.getLogger("bounded"), limit=2, level=level)
    with caplog.at_level(logging.DEBUG, logger="bounded"):
        for value in ("a", "a", "b", "c", "d", "a"):
            bad.log(value, "bad %s", value)
    assert [r.getMessage() for r in caplog.records] == [
        "bad a",
        "bad b",
        "2 values were reported; further ones are not logged",
    ]
    assert {r.levelno for r in caplog.records} == {level}
