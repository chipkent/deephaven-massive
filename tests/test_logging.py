"""Tests for the logging switch: `enable_debug_logging` and the two environment variables.

Each test starts from a fresh switch (nothing installed, the variables unread) and the
`deephaven_massive` logger's own level and handlers, and puts both back afterwards, so no
handler a test installs outlives it.
"""

from __future__ import annotations

import logging
import re
import threading
from typing import TYPE_CHECKING

import pytest

import deephaven_massive as dm
from deephaven_massive import _logging

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

LIBRARY = logging.getLogger("deephaven_massive")
CHILD = logging.getLogger("deephaven_massive._query")
# `2026-10-28 14:03:07,112Z DEBUG [MainThread] deephaven_massive._query: a message`
LINE = re.compile(r"^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d,\d{3}Z (\w+) \[([^\]]+)\] ([\w.]+): (.*)$")


@pytest.fixture(autouse=True)
def fresh_switch(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setattr(_logging, "_switch", _logging._Switch())
    monkeypatch.delenv(_logging.LEVEL_VARIABLE, raising=False)
    monkeypatch.delenv(_logging.FILE_VARIABLE, raising=False)
    level, handlers = LIBRARY.level, list(LIBRARY.handlers)
    yield
    for handler in [h for h in LIBRARY.handlers if h not in handlers]:
        LIBRARY.removeHandler(handler)
        handler.close()
    LIBRARY.setLevel(level)


def _lines(text: str) -> list[tuple[str, str, str, str]]:
    # Parse every line the handler wrote into (level, thread, logger, message).
    parsed = []
    for line in text.splitlines():
        match = LINE.match(line)
        assert match, line
        level, thread, name, message = match.groups()
        parsed.append((level, thread, name, message))
    return parsed


def test_nothing_below_warning_is_enabled_until_the_log_is_turned_on() -> None:
    assert LIBRARY.level == logging.NOTSET
    assert not CHILD.isEnabledFor(logging.DEBUG)
    assert not CHILD.isEnabledFor(logging.INFO)


def test_enable_debug_logging_writes_every_level_to_stderr(
    capsys: pytest.CaptureFixture[str],
) -> None:
    dm.enable_debug_logging()
    CHILD.debug("call %d start", 7)
    assert _lines(capsys.readouterr().err) == [
        (
            "INFO",
            "MainThread",
            "deephaven_massive._logging",
            "logging deephaven_massive at DEBUG to stderr (from enable_debug_logging())",
        ),
        ("DEBUG", "MainThread", "deephaven_massive._query", "call 7 start"),
    ]
    # Records still reach the root's handlers, such as pytest's own.
    assert LIBRARY.propagate


def test_the_line_names_the_thread_that_logged_it(capsys: pytest.CaptureFixture[str]) -> None:
    dm.enable_debug_logging()
    capsys.readouterr()
    worker = threading.Thread(target=CHILD.debug, args=("from a worker",), name="massive #9 reaper")
    worker.start()
    worker.join()
    assert _lines(capsys.readouterr().err) == [
        ("DEBUG", "massive #9 reaper", "deephaven_massive._query", "from a worker")
    ]


def test_times_are_utc() -> None:
    dm.enable_debug_logging()
    [handler] = LIBRARY.handlers
    record = logging.LogRecord(CHILD.name, logging.DEBUG, __file__, 1, "at midnight", None, None)
    # 2021-01-01 00:00:00 UTC, still 2020 in every zone west of Greenwich.
    record.created, record.msecs = 1_609_459_200.0, 0.0
    assert handler.format(record).startswith("2021-01-01 00:00:00,000Z DEBUG ")


def test_enable_debug_logging_appends_to_a_file(tmp_path: Path) -> None:
    path = tmp_path / "massive.log"
    path.write_text("an earlier line\n", encoding="utf-8")
    dm.enable_debug_logging(path)
    CHILD.debug("über %s", "AAPL")
    lines = path.read_text(encoding="utf-8").splitlines()
    assert lines[0] == "an earlier line"
    assert _lines("\n".join(lines[1:]))[-1] == (
        "DEBUG",
        "MainThread",
        "deephaven_massive._query",
        "über AAPL",
    )
    assert f"to {path} (from enable_debug_logging())" in lines[1]


def test_a_second_call_replaces_and_closes_the_first_handler(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    first = tmp_path / "first.log"
    dm.enable_debug_logging(first)
    [handler] = [h for h in LIBRARY.handlers if isinstance(h, logging.FileHandler)]
    dm.enable_debug_logging()
    CHILD.debug("after the switch")
    assert handler not in LIBRARY.handlers
    assert handler.stream is None  # closed
    assert "after the switch" not in first.read_text(encoding="utf-8")
    assert "after the switch" in capsys.readouterr().err


def test_a_file_that_cannot_be_opened_keeps_the_previous_handler(tmp_path: Path) -> None:
    dm.enable_debug_logging()
    handlers = list(LIBRARY.handlers)
    with pytest.raises(OSError, match="missing"):
        dm.enable_debug_logging(tmp_path / "missing" / "massive.log")
    assert LIBRARY.handlers == handlers


@pytest.mark.parametrize(
    ("value", "level"),
    [("DEBUG", logging.DEBUG), ("debug", logging.DEBUG), (" Info ", logging.INFO)],
)
def test_the_environment_variable_turns_the_log_on(
    *, value: str, level: int, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("DEEPHAVEN_MASSIVE_LOG", value)
    _logging.configure_from_environment()
    assert LIBRARY.level == level

    CHILD.info("an INFO line")
    assert _lines(capsys.readouterr().err)[-1][3] == "an INFO line"


def test_the_file_variable_sends_the_lines_to_a_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "massive.log"
    monkeypatch.setenv("DEEPHAVEN_MASSIVE_LOG", "DEBUG")
    monkeypatch.setenv("DEEPHAVEN_MASSIVE_LOG_FILE", str(path))
    _logging.configure_from_environment()
    CHILD.debug("into the file")
    text = path.read_text(encoding="utf-8")
    assert "logging deephaven_massive at DEBUG to " in text
    assert "(from the environment)" in text
    assert "into the file" in text
    assert capsys.readouterr().err == ""


@pytest.mark.parametrize("value", ["", "  "])
def test_an_unset_or_blank_level_changes_nothing(
    value: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("DEEPHAVEN_MASSIVE_LOG", value)
    # The file variable alone does nothing.
    monkeypatch.setenv("DEEPHAVEN_MASSIVE_LOG_FILE", str(tmp_path / "massive.log"))
    handlers = list(LIBRARY.handlers)
    _logging.configure_from_environment()
    assert LIBRARY.handlers == handlers
    assert LIBRARY.level == logging.NOTSET
    assert not (tmp_path / "massive.log").exists()


def test_an_unknown_level_names_the_variable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DEEPHAVEN_MASSIVE_LOG", "verbose")
    message = (
        "DEEPHAVEN_MASSIVE_LOG='verbose' is not a logging level; use one of DEBUG, INFO, "
        "WARNING, ERROR, CRITICAL"
    )
    with pytest.raises(ValueError, match=f"^{re.escape(message)}$"):
        _logging.configure_from_environment()
    # The variables count as read: a later session does not raise again.
    _logging.configure_from_environment()


def test_the_variables_are_read_once(monkeypatch: pytest.MonkeyPatch) -> None:
    _logging.configure_from_environment()
    monkeypatch.setenv("DEEPHAVEN_MASSIVE_LOG", "DEBUG")
    _logging.configure_from_environment()
    assert LIBRARY.level == logging.NOTSET


def test_the_function_wins_over_the_variables(monkeypatch: pytest.MonkeyPatch) -> None:
    dm.enable_debug_logging()
    handlers = list(LIBRARY.handlers)
    monkeypatch.setenv("DEEPHAVEN_MASSIVE_LOG", "ERROR")
    _logging.configure_from_environment()
    assert (LIBRARY.handlers, LIBRARY.level) == (handlers, logging.DEBUG)


def test_the_variables_lose_a_race_with_the_function(tmp_path: Path) -> None:
    # The function ran while the variables were being read, after their flag was taken: the
    # handler the variables built is closed instead of installed.
    dm.enable_debug_logging()
    handlers = list(LIBRARY.handlers)
    path = tmp_path / "massive.log"
    _logging._install(logging.ERROR, path, environment=True)
    assert (LIBRARY.handlers, LIBRARY.level) == (handlers, logging.DEBUG)
    assert path.read_text(encoding="utf-8") == ""
