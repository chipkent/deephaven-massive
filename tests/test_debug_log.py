"""Tests for the logging switch: `enable_debug_logging` and the two environment variables.

Each test starts from a fresh switch (nothing installed, the variables unread) and the
`deephaven_massive` logger's own level and handlers, and puts both back afterwards, so no
handler a test installs outlives it.
"""

from __future__ import annotations

import importlib.metadata
import logging
import platform
import re
import sys
import threading
from datetime import datetime
from typing import TYPE_CHECKING, cast

import pytest

import deephaven_massive as dm
from deephaven_massive import _debug_log
from deephaven_massive._massive import provenance

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

LIBRARY = logging.getLogger("deephaven_massive")
CHILD = logging.getLogger("deephaven_massive.queries._query")
# `2026-10-28 14:03:07,112Z DEBUG [MainThread] deephaven_massive.queries._query: a message`
LINE = re.compile(r"^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d,\d{3}Z (\w+) \[([^\]]+)\] ([\w.]+): (.*)$")


@pytest.fixture(autouse=True)
def fresh_switch(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setattr(_debug_log, "_switch", _debug_log._Switch())
    monkeypatch.delenv(_debug_log.LEVEL_VARIABLE, raising=False)
    monkeypatch.delenv(_debug_log.FILE_VARIABLE, raising=False)
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
    lines = _lines(capsys.readouterr().err)
    assert lines[0] == (
        "INFO",
        "MainThread",
        "deephaven_massive._debug_log",
        "logging deephaven_massive at DEBUG to stderr (from enable_debug_logging())",
    )
    assert lines[1][3].startswith("deephaven-massive ")  # the process, described once
    queries = "deephaven_massive.queries._query"
    assert lines[2:] == [("DEBUG", "MainThread", queries, "call 7 start")]
    # Records still reach the root's handlers, such as pytest's own.
    assert LIBRARY.propagate


def test_the_line_names_the_thread_that_logged_it(capsys: pytest.CaptureFixture[str]) -> None:
    dm.enable_debug_logging()
    capsys.readouterr()
    worker = threading.Thread(target=CHILD.debug, args=("from a worker",), name="massive #9 reaper")
    worker.start()
    worker.join()
    assert _lines(capsys.readouterr().err) == [
        ("DEBUG", "massive #9 reaper", "deephaven_massive.queries._query", "from a worker")
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
        "deephaven_massive.queries._query",
        "über AAPL",
    )
    assert f"to {path.resolve()} (from enable_debug_logging())" in lines[1]


def test_the_line_names_the_resolved_path_of_a_relative_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "real").mkdir()
    (tmp_path / "link").symlink_to(tmp_path / "real")
    monkeypatch.chdir(tmp_path)
    dm.enable_debug_logging("link/massive.log")
    text = (tmp_path / "real" / "massive.log").read_text(encoding="utf-8")
    resolved = (tmp_path / "real" / "massive.log").resolve()
    assert f"logging deephaven_massive at DEBUG to {resolved} (from enable_debug_logging())" in text


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
    [("DEBUG", logging.DEBUG), ("debug", logging.DEBUG), ("Info", logging.INFO)],
)
def test_the_environment_variable_turns_the_log_on(
    *, value: str, level: int, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("DEEPHAVEN_MASSIVE_LOG", value)
    _debug_log.configure_from_environment()
    assert LIBRARY.level == level

    CHILD.info("an INFO line")
    assert _lines(capsys.readouterr().err)[-1][3] == "an INFO line"


def test_the_file_variable_sends_the_lines_to_a_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "massive.log"
    monkeypatch.setenv("DEEPHAVEN_MASSIVE_LOG", "DEBUG")
    monkeypatch.setenv("DEEPHAVEN_MASSIVE_LOG_FILE", str(path))
    _debug_log.configure_from_environment()
    CHILD.debug("into the file")
    text = path.read_text(encoding="utf-8")
    assert "logging deephaven_massive at DEBUG to " in text
    assert "(from the environment)" in text
    assert "into the file" in text
    assert capsys.readouterr().err == ""


@pytest.mark.parametrize("value", ["", "  "])
def test_an_unset_or_blank_level_and_file_change_nothing(
    value: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DEEPHAVEN_MASSIVE_LOG", value)
    monkeypatch.setenv("DEEPHAVEN_MASSIVE_LOG_FILE", value)
    handlers = list(LIBRARY.handlers)
    _debug_log.configure_from_environment()
    assert LIBRARY.handlers == handlers
    assert LIBRARY.level == logging.NOTSET


@pytest.mark.parametrize("value", [None, "", "  "])
def test_the_file_variable_alone_logs_at_debug(
    value: str | None, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    if value is not None:
        monkeypatch.setenv("DEEPHAVEN_MASSIVE_LOG", value)
    path = tmp_path / "massive.log"
    monkeypatch.setenv("DEEPHAVEN_MASSIVE_LOG_FILE", str(path))
    _debug_log.configure_from_environment()
    assert LIBRARY.level == logging.DEBUG
    CHILD.debug("a DEBUG line")
    text = path.read_text(encoding="utf-8")
    assert (
        f"logging deephaven_massive at DEBUG to {path.resolve()} "
        "(from the environment; DEEPHAVEN_MASSIVE_LOG unset, so DEBUG)"
    ) in text
    assert _lines(text)[-1][3] == "a DEBUG line"


@pytest.mark.parametrize("variable", ["DEEPHAVEN_MASSIVE_LOG", "DEEPHAVEN_MASSIVE_LOG_FILE"])
def test_a_variable_with_surrounding_whitespace_is_refused(
    monkeypatch: pytest.MonkeyPatch, variable: str
) -> None:
    monkeypatch.setenv(variable, " INFO ")
    message = f"{variable}=' INFO ' has leading or trailing whitespace; set it without any"
    with pytest.raises(ValueError, match=f"^{re.escape(message)}$"):
        _debug_log.configure_from_environment()


def test_an_unknown_level_names_the_variable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DEEPHAVEN_MASSIVE_LOG", "verbose")
    message = (
        "DEEPHAVEN_MASSIVE_LOG='verbose' is not a logging level; use one of DEBUG, INFO, "
        "WARN, WARNING, ERROR, CRITICAL"
    )
    with pytest.raises(ValueError, match=f"^{re.escape(message)}$"):
        _debug_log.configure_from_environment()
    # The variables count as read: a later session does not raise again.
    _debug_log.configure_from_environment()


def test_the_variables_are_read_once(monkeypatch: pytest.MonkeyPatch) -> None:
    _debug_log.configure_from_environment()
    monkeypatch.setenv("DEEPHAVEN_MASSIVE_LOG", "DEBUG")
    _debug_log.configure_from_environment()
    assert LIBRARY.level == logging.NOTSET


def test_the_function_wins_over_the_variables(monkeypatch: pytest.MonkeyPatch) -> None:
    dm.enable_debug_logging()
    handlers = list(LIBRARY.handlers)
    monkeypatch.setenv("DEEPHAVEN_MASSIVE_LOG", "ERROR")
    _debug_log.configure_from_environment()
    assert (LIBRARY.handlers, LIBRARY.level) == (handlers, logging.DEBUG)


def test_the_variables_lose_a_race_with_the_function(tmp_path: Path) -> None:
    # The function ran while the variables were being read, after their flag was taken: the
    # handler the variables built is closed instead of installed.
    dm.enable_debug_logging()
    handlers = list(LIBRARY.handlers)
    path = tmp_path / "massive.log"
    _debug_log._install(logging.ERROR, path, environment=True, source="the environment")
    assert (LIBRARY.handlers, LIBRARY.level) == (handlers, logging.DEBUG)
    assert path.read_text(encoding="utf-8") == ""


@pytest.mark.parametrize("value", ["WARN", "warn"])
def test_warn_is_a_level_name(value: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DEEPHAVEN_MASSIVE_LOG", value)
    _debug_log.configure_from_environment()
    assert LIBRARY.level == logging.WARNING


def test_a_log_file_that_cannot_be_opened_names_the_variable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "missing" / "massive.log"
    monkeypatch.setenv("DEEPHAVEN_MASSIVE_LOG", "DEBUG")
    monkeypatch.setenv("DEEPHAVEN_MASSIVE_LOG_FILE", str(path))
    message = f"DEEPHAVEN_MASSIVE_LOG_FILE={str(path)!r} cannot be opened for appending: "
    with pytest.raises(OSError, match=f"^{re.escape(message)}") as info:
        _debug_log.configure_from_environment()
    assert isinstance(info.value.__cause__, FileNotFoundError)
    assert not [h for h in LIBRARY.handlers if isinstance(h, logging.FileHandler)]


def test_filename_must_be_a_path_or_none() -> None:
    with pytest.raises(
        TypeError, match=r"^filename must be a file path \(str or os\.PathLike\) or None, not int$"
    ):
        dm.enable_debug_logging(cast("str", 3))


@pytest.mark.parametrize("proxy", [False, True])
def test_turning_the_log_on_describes_the_process(
    *, proxy: bool, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    for name in ("HTTPS_PROXY", "https_proxy", "SSL_CERT_FILE"):
        monkeypatch.delenv(name, raising=False)
    if proxy:
        monkeypatch.setenv("HTTPS_PROXY", "http://user:proxy-password@proxy:3128")
        monkeypatch.setenv("SSL_CERT_FILE", "/etc/ssl/bundle.pem")
    with caplog.at_level(logging.INFO, logger=_debug_log.__name__):
        dm.enable_debug_logging(filename=None)
    [line] = [m for m in caplog.messages if m.startswith("deephaven-massive ")]
    gil = "on" if getattr(sys, "_is_gil_enabled", lambda: True)() else "off"
    setting = "set" if proxy else "unset"
    local = datetime.now().astimezone()
    assert line.startswith(
        f"deephaven-massive {dm.__version__} (git commit {_debug_log._commit()}), "
        f"Python {sys.version.split()[0]} (GIL {gil}), {platform.platform()}, "
        f"local time UTC{local.strftime('%z')} ({local.tzname()}); "
    )
    for part in (
        f"pyarrow {importlib.metadata.version('pyarrow')}",
        "orjson ",
        f"; Massive spec snapshot of {provenance().fetched}; ",
    ):
        assert part in line
    assert line.endswith(f"HTTPS_PROXY {setting}, SSL_CERT_FILE {setting}")
    assert "proxy-password" not in line
    assert "bundle.pem" not in line


def test_the_variables_describe_the_process_too(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setenv("DEEPHAVEN_MASSIVE_LOG", "INFO")
    with caplog.at_level(logging.INFO, logger=_debug_log.__name__):
        _debug_log.configure_from_environment()
    assert [m for m in caplog.messages if m.startswith("deephaven-massive ")]


def test_nothing_describes_the_process_when_info_is_off(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setenv("DEEPHAVEN_MASSIVE_LOG", "ERROR")
    with caplog.at_level(logging.ERROR, logger=_debug_log.__name__):
        _debug_log.configure_from_environment()
    assert caplog.messages == []


class _Distribution:
    # Stands in for an installed distribution whose `direct_url.json` holds `text`.
    def __init__(self, text: str | None) -> None:
        self.text = text

    def read_text(self, filename: str) -> str | None:
        assert filename == "direct_url.json"
        return self.text


def test_the_commit_comes_from_the_install_record(monkeypatch: pytest.MonkeyPatch) -> None:
    text = '{"url": "https://github.com/x/y", "vcs_info": {"vcs": "git", "commit_id": "abc123"}}'
    monkeypatch.setattr(_debug_log, "distribution", lambda _name: _Distribution(text))
    assert _debug_log._commit() == "abc123"


@pytest.mark.parametrize(
    "text",
    [
        None,  # no direct_url.json: installed from an index
        '{"url": "file:///src", "dir_info": {"editable": true}}',  # a local directory
        '{"vcs_info": {"vcs": "git"}}',  # no commit_id
        '{"vcs_info": {"commit_id": ""}}',
        '{"vcs_info": {"commit_id": 7}}',
        "not json",
        "[1, 2]",  # JSON, but not an object
    ],
)
def test_a_commit_that_is_not_recorded_says_so(
    text: str | None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(_debug_log, "distribution", lambda _name: _Distribution(text))
    assert _debug_log._commit() == "not recorded"


def test_a_missing_distribution_has_no_commit(monkeypatch: pytest.MonkeyPatch) -> None:
    def missing(name: str) -> _Distribution:
        raise importlib.metadata.PackageNotFoundError(name)

    monkeypatch.setattr(_debug_log, "distribution", missing)
    assert _debug_log._commit() == "not recorded"


def test_a_missing_distribution_is_named_not_installed() -> None:
    assert _debug_log._version("no-such-distribution-of-this-name") == "not installed"


def test_disable_debug_logging_closes_the_handler_and_restores_the_level(
    tmp_path: Path,
) -> None:
    path = tmp_path / "massive.log"
    dm.enable_debug_logging(path)
    [handler] = [h for h in LIBRARY.handlers if isinstance(h, logging.FileHandler)]
    dm.disable_debug_logging()
    CHILD.debug("after the switch")
    assert handler not in LIBRARY.handlers
    assert handler.stream is None  # closed
    assert LIBRARY.level == logging.NOTSET
    text = path.read_text(encoding="utf-8")
    # The log says where it ended, and nothing after.
    assert _lines(text)[-1][2:] == (
        "deephaven_massive._debug_log",
        "logging deephaven_massive off (from disable_debug_logging())",
    )
    assert "after the switch" not in text
    assert not CHILD.isEnabledFor(logging.DEBUG)


def test_disable_debug_logging_turns_off_the_environment_switch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DEEPHAVEN_MASSIVE_LOG", "INFO")
    _debug_log.configure_from_environment()
    assert LIBRARY.level == logging.INFO
    dm.disable_debug_logging()
    assert (LIBRARY.level, _debug_log._switch.handler) == (logging.NOTSET, None)


def test_disable_debug_logging_before_the_first_session_keeps_the_variables_off(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dm.disable_debug_logging()  # nothing is on: no change, and no line
    assert (LIBRARY.level, _debug_log._switch.handler) == (logging.NOTSET, None)
    monkeypatch.setenv("DEEPHAVEN_MASSIVE_LOG", "DEBUG")
    _debug_log.configure_from_environment()
    assert LIBRARY.level == logging.NOTSET
    dm.enable_debug_logging()  # the function still turns it on
    assert LIBRARY.level == logging.DEBUG


def test_disable_debug_logging_loses_a_race_with_enable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The log is turned on again between the off line and the removal: the new handler stays.
    dm.enable_debug_logging()
    path = tmp_path / "massive.log"
    off_line = _debug_log.logger.info

    def turn_on_again(*args: object) -> None:
        off_line(*args)
        monkeypatch.setattr(_debug_log.logger, "info", off_line)
        dm.enable_debug_logging(path)

    monkeypatch.setattr(_debug_log.logger, "info", turn_on_again)
    dm.disable_debug_logging()
    [handler] = [h for h in LIBRARY.handlers if isinstance(h, logging.FileHandler)]
    assert _debug_log._switch.handler is handler
    assert LIBRARY.level == logging.DEBUG


def test_disable_debug_logging_wins_over_an_environment_read_in_flight(tmp_path: Path) -> None:
    # The first session read the variables, then disable ran before their handler went in:
    # the handler the variables built is closed instead of installed.
    dm.disable_debug_logging()
    path = tmp_path / "massive.log"
    _debug_log._install(logging.DEBUG, path, environment=True, source="the environment")
    assert (LIBRARY.level, _debug_log._switch.handler) == (logging.NOTSET, None)
    assert path.read_text(encoding="utf-8") == ""
