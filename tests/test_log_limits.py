import logging
import threading

import pytest

from deephaven_massive import _log_limits
from deephaven_massive._errors import LIBRARY_BUG, ErrorSource, MassiveError
from deephaven_massive._log_limits import (
    LIST_LIMIT,
    BoundedLog,
    LoggedKeys,
    bounded_list,
    counted,
    log_failure,
    log_once,
    mark_failure_logged,
    prose_list,
)


def test_bounded_list_shows_the_count_and_the_first_items() -> None:
    assert bounded_list([]) == "0 []"
    assert bounded_list(["AAPL", "MSFT"]) == "2 [AAPL, MSFT]"
    assert bounded_list(iter([7, 9])) == "2 [7, 9]"  # any iterable, each item by str
    assert bounded_list("ABCDE", limit=3) == "5 [A, B, C, +2 more]"
    assert bounded_list("ABC", limit=3) == "3 [A, B, C]"  # exactly the limit: none left out
    many = [f"T{n}" for n in range(LIST_LIMIT + 1)]
    assert bounded_list(many).endswith(f", T{LIST_LIMIT - 1}, +1 more]")


def test_prose_list_joins_a_few_items_as_prose() -> None:
    assert prose_list([], "symbols") == "no symbols"
    assert prose_list(["X:FOOUSD"], "symbols") == "X:FOOUSD"
    assert prose_list(["X:FOOUSD", "X:BARUSD"], "symbols") == "X:FOOUSD and X:BARUSD"
    assert prose_list(["AAPL", "MSFT", "NVDA"], "symbols") == "AAPL, MSFT, and NVDA"
    assert prose_list(iter([7, 9]), "ids") == "7 and 9"  # any iterable, each item by str


def test_prose_list_counts_a_long_list_and_shows_its_first_items() -> None:
    symbols = [f"S{n}" for n in range(12)]
    assert prose_list(symbols, "symbols") == "12 symbols, the first 3: S0, S1, and S2"
    assert prose_list(symbols, "symbols", shown=1) == "12 symbols, the first 1: S0"
    assert prose_list(symbols[:3], "symbols", shown=3) == "S0, S1, and S2"  # exactly the limit


def test_counted_gives_the_singular_for_one_and_separates_thousands() -> None:
    assert counted(0, "row") == "0 rows"
    assert counted(1, "row") == "1 row"
    assert counted(3, "row") == "3 rows"
    assert counted(12345, "ticker") == "12,345 tickers"


def test_log_once_logs_each_key_once(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(_log_limits, "_logged", LoggedKeys())
    log = logging.getLogger("test.log_once")
    with caplog.at_level(logging.DEBUG, logger="test.log_once"):
        log_once(log, "a", "first %s", 1)
        log_once(log, "a", "again %s", 2)
        log_once(log, "b", "other", level=logging.WARNING)
    assert [(r.levelno, r.getMessage()) for r in caplog.records] == [
        (logging.INFO, "first 1"),
        (logging.WARNING, "other"),
    ]


def test_logged_keys_mark_new_keys_for_their_owner_only(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(_log_limits, "_logged", LoggedKeys())
    logged = LoggedKeys()
    assert logged.first_seen(["a", "b", "a"]) == ["a", "b"]
    assert logged.first_seen(["b", "c"]) == ["c"]
    assert LoggedKeys().first_seen(["a"]) == ["a"]  # another owner has seen nothing
    log = logging.getLogger("test.logged_keys")
    with caplog.at_level(logging.INFO, logger="test.logged_keys"):
        log_once(log, "a", "the process has not logged it")  # an owner's keys are its own
    assert caplog.messages == ["the process has not logged it"]


def test_log_once_is_once_across_threads(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(_log_limits, "_logged", LoggedKeys())
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
        "2 values were reported. Further ones are not logged.",
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
