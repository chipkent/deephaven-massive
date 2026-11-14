import logging
import threading

import pytest

from deephaven_massive import _log_limits
from deephaven_massive._errors import LIBRARY_BUG, ErrorSource, MassiveError
from deephaven_massive._log_limits import (
    LIST_LIMIT,
    BoundedLog,
    bounded_list,
    first_seen,
    log_failure,
    log_once,
    mark_failure_logged,
)


def test_bounded_list_shows_the_count_and_the_first_items() -> None:
    assert bounded_list([]) == "0 []"
    assert bounded_list(["AAPL", "MSFT"]) == "2 [AAPL, MSFT]"
    assert bounded_list(iter([7, 9])) == "2 [7, 9]"  # any iterable, each item by str
    assert bounded_list("ABCDE", limit=3) == "5 [A, B, C, +2 more]"
    assert bounded_list("ABC", limit=3) == "3 [A, B, C]"  # exactly the limit: none left out
    many = [f"T{n}" for n in range(LIST_LIMIT + 1)]
    assert bounded_list(many).endswith(f", T{LIST_LIMIT - 1}, +1 more]")


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


def test_first_seen_marks_new_keys_shared_with_log_once(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(_log_limits, "_logged", set())
    assert first_seen(["a", "b", "a"]) == ["a", "b"]
    assert first_seen(["b", "c"]) == ["c"]
    log = logging.getLogger("test.first_seen")
    with caplog.at_level(logging.INFO, logger="test.first_seen"):
        log_once(log, "a", "already seen")
    assert caplog.records == []


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


def test_a_failure_is_logged_once_however_many_tables_it_fails(
    caplog: pytest.LogCaptureFixture,
) -> None:
    log = logging.getLogger("failures")
    refused = MassiveError("refused")
    with caplog.at_level(logging.DEBUG, logger="failures"):
        assert log_failure(log, refused, "stream %d failed: %s", 1)
        assert not log_failure(log, refused, "stream %d failed: %s", 2)
    assert [(r.levelno, r.getMessage()) for r in caplog.records] == [
        (logging.WARNING, "stream 1 failed: refused"),
        (logging.DEBUG, "stream 2 failed: refused"),
    ]


@pytest.mark.parametrize(
    ("error", "level", "text"),
    [
        (MassiveError("refused"), logging.DEBUG, "refused"),  # the caller's chosen level
        (MassiveError("fix it", source=ErrorSource.USER, is_bug=True), logging.ERROR, "fix it"),
        (MassiveError("no key", source=ErrorSource.USER), logging.DEBUG, "no key"),  # no bug
        (
            MassiveError("row broke", source=ErrorSource.LIBRARY),
            logging.ERROR,
            f"row broke. {LIBRARY_BUG}",
        ),
        (KeyError("k"), logging.ERROR, f"KeyError: 'k'. {LIBRARY_BUG}"),
    ],
)
def test_a_failure_is_logged_at_error_only_when_it_is_a_bug(
    caplog: pytest.LogCaptureFixture, *, error: BaseException, level: int, text: str
) -> None:
    log = logging.getLogger("causes")
    with caplog.at_level(logging.DEBUG, logger="causes"):
        assert log_failure(log, error, "failed: %s", expected=logging.DEBUG, exc_info=error)
    [record] = caplog.records
    assert (record.levelno, record.getMessage()) == (level, f"failed: {text}")
    assert record.exc_info is not None


@pytest.mark.parametrize(
    ("error", "traced"),
    [
        (MassiveError("broke", source=ErrorSource.LIBRARY), True),
        (MassiveError("fix it", source=ErrorSource.USER, is_bug=True), True),
        (KeyError("k"), True),
        (MassiveError("refused"), False),
    ],
)
def test_a_bug_is_logged_with_its_traceback_though_the_caller_passes_none(
    caplog: pytest.LogCaptureFixture, *, error: BaseException, traced: bool
) -> None:
    with caplog.at_level(logging.DEBUG, logger="traced"):
        log_failure(logging.getLogger("traced"), error, "failed: %s")
    [record] = caplog.records
    assert (record.exc_info is not None) is traced


def test_a_failure_marked_as_logged_is_logged_by_log_failure_at_debug_only(
    caplog: pytest.LogCaptureFixture,
) -> None:
    defect = MassiveError("broke", source=ErrorSource.LIBRARY)
    assert mark_failure_logged(defect)
    assert not mark_failure_logged(defect)
    assert mark_failure_logged(KeyError("k"))  # only a MassiveError is remembered
    with caplog.at_level(logging.DEBUG, logger="marked"):
        log_failure(logging.getLogger("marked"), defect, "%s", exc_info=defect)
    [record] = caplog.records
    assert (record.levelno, record.exc_info) == (logging.DEBUG, None)
