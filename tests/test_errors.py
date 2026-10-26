import logging
import threading

import pytest

from deephaven_massive import _errors
from deephaven_massive._errors import (
    AuthenticationError,
    EntitlementError,
    MassiveError,
    log_once,
)


@pytest.mark.parametrize("error", [AuthenticationError, EntitlementError])
def test_specific_errors_are_massive_errors(error: type[MassiveError]) -> None:
    with pytest.raises(MassiveError, match="detail"):
        raise error("detail")


def test_errors_are_distinct() -> None:
    assert not issubclass(AuthenticationError, EntitlementError)
    assert not issubclass(EntitlementError, AuthenticationError)


def test_log_once_logs_each_key_once(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(_errors, "_logged", set())
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
    monkeypatch.setattr(_errors, "_logged", set())
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
