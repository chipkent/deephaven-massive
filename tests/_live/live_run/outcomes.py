"""The live run's record of each test phase's outcome, written as the tests run.

The live-run program (`live_run`) runs the live suites in child pytest processes, loading this
module as a plugin (`-p tests._live.live_run.outcomes`). For every phase of every test (setup, call,
teardown) the plugin appends one JSON line to the file its option `--live-outcomes` names,
and flushes it, so the record survives a child that is killed, crashes, or ends the JVM: pytest's
own reports would be lost with the process. Each line holds:

- `nodeid`, `phase` (`setup`, `call`, or `teardown`), and `outcome` (`passed`, `failed`, or
  `skipped`);
- `reason`, a skip's reason without pytest's `Skipped: ` prefix, else `""`;
- `error_type`, the class name of a failure's exception (such as `AssertionError`), else `""`;
- `items`, the open verification items the test answers (its `settles` and `verifies`
  markers), collected once per run, since a report carries no markers;
- `issue`, for a check of an open Massive problem, the GitHub issue its `massive_issue` marker
  names, else `null`;
- `problem`, for such a check's call, `present` when it failed as expected and `fixed` when it
  passed against its expectation (`massive_problems.judge`), else `""`; the `reason` of a
  present one is the values Massive sent (the `ProblemShownError` message).

At collection, with its option `--live-collection` given, the plugin also writes that file: a
JSON object of every collected test's items and issue, from which the program lists a group's
checks. The plugin adds the option `--live-run-token`, which only marks the child's command
line so the program can tell it from another process (`children.is_alive`).

Without either file option the plugin writes nothing. `read_outcomes` reads the file back
for the program, which turns each test's phases into one check status (`check_outcome`).
"""

from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final

import pytest

from tests._live.massive_problems import FIXED, PRESENT, issue_of, judge
from tests._live.summary import REFUSED

if TYPE_CHECKING:
    from collections.abc import Generator, Iterable

OUTCOMES_OPTION: Final = "--live-outcomes"
"""The option that names the file the plugin appends to."""

COLLECTION_OPTION: Final = "--live-collection"
"""The option that names the file the plugin writes the collected tests to."""

PROBLEM_FIXED_REASON: Final = "the check passed: Massive fixed the problem, so close its issue"
"""The reason of a `problem fixed` check."""

_ITEMS: dict[str, list[str]] = {}
"""Each collected test's open-question items, by node id."""

_ISSUES: dict[str, int | None] = {}
"""Each collected test's GitHub issue of a Massive problem (`massive_issue`), by node id."""

_LOCK = threading.Lock()
"""Serializes the appends, since a report may come from a timeout thread."""


def pytest_addoption(parser: pytest.Parser) -> None:
    """Add `--live-run-token`, which marks a live-run child's command line, and the file options.

    Args:
        parser: pytest's option parser.
    """
    parser.addoption(
        "--live-run-token",
        default="",
        help="a random token the live-run program puts on its child's command line, to "
        "recognize the child later; it changes nothing in the run",
    )
    parser.addoption(
        OUTCOMES_OPTION,
        default="",
        metavar="PATH",
        help="append one JSON line per phase of every test to this file, as it runs",
    )
    parser.addoption(
        COLLECTION_OPTION,
        default="",
        metavar="PATH",
        help="write every collected test's open verification items to this file",
    )


@pytest.hookimpl(trylast=True)
def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Keep each test's items and issue, and write the collection when the program asks for it.

    Runs last, after `tests/conftest.py` has deselected the tests the run leaves out.

    Args:
        config: The pytest configuration, which holds `--live-collection`.
        items: The collected tests.
    """
    collection: dict[str, dict[str, Any]] = {}
    for item in items:
        _ITEMS[item.nodeid] = [
            str(arg)
            for name in ("settles", "verifies")
            for marker in item.iter_markers(name)
            for arg in marker.args
        ]
        _ISSUES[item.nodeid] = issue_of(item)
        collection[item.nodeid] = {"items": _ITEMS[item.nodeid], "issue": _ISSUES[item.nodeid]}
    target = str(config.getoption(COLLECTION_OPTION))
    if target:
        Path(target).write_text(json.dumps(collection, indent=1) + "\n", encoding="utf-8")


@pytest.hookimpl(wrapper=True)
def pytest_runtest_makereport(
    item: pytest.Item, call: pytest.CallInfo[None]
) -> Generator[None, pytest.TestReport, pytest.TestReport]:
    """Append the phase's outcome once its report is final.

    Args:
        item: The test.
        call: The phase's result, whose exception names a failure's type.

    Yields:
        Control to the report's creation.

    Returns:
        The report, unchanged.
    """
    report = yield
    target = str(item.config.getoption(OUTCOMES_OPTION))
    if target:
        error_type = call.excinfo.typename if report.failed and call.excinfo is not None else ""
        problem = judge(report, call) if _ISSUES.get(item.nodeid) is not None else ""
        if problem == PRESENT and call.excinfo is not None:
            reason = str(call.excinfo.value)
        elif report.skipped:
            reason = skip_reason(report.longrepr)
        else:
            reason = ""
        line = {
            "nodeid": item.nodeid,
            "phase": report.when,
            "outcome": report.outcome,
            "reason": reason,
            "error_type": error_type,
            "items": _ITEMS.get(item.nodeid, []),
            "issue": _ISSUES.get(item.nodeid),
            "problem": problem,
        }
        append_line(Path(target), line)
    return report


def skip_reason(longrepr: object) -> str:
    """Return a skip's reason from its report's `longrepr`.

    Args:
        longrepr: A skipped report's `longrepr`: a `(path, line, reason)` tuple, or text.

    Returns:
        The reason, without pytest's `Skipped: ` prefix.
    """
    reason = str(longrepr[2]) if isinstance(longrepr, tuple) else str(longrepr)
    return reason.removeprefix("Skipped: ")


def append_line(path: Path, line: dict[str, Any]) -> None:
    """Append one JSON line to the outcomes file and flush it to the operating system.

    Args:
        path: The outcomes file; created if missing.
        line: The record.
    """
    text = json.dumps(line, sort_keys=True) + "\n"
    with _LOCK, path.open("a", encoding="utf-8") as file:
        file.write(text)
        file.flush()


def read_outcomes(path: Path) -> list[dict[str, Any]]:
    """Read an outcomes file, skipping a last line cut short by a killed child.

    Args:
        path: The outcomes file.

    Returns:
        Its records in order; empty when the file is missing.
    """
    if not path.exists():
        return []
    records = []
    for text in path.read_text(encoding="utf-8").splitlines():
        try:
            records.append(json.loads(text))
        except ValueError:
            continue
    return records


def check_outcome(phases: Iterable[dict[str, Any]]) -> tuple[str, str]:
    """Turn one test's phase records into its check status and reason.

    A test failed when any phase failed, other than the call of a check of an open Massive
    problem that passed against its expectation (pytest reports it as a failure). Otherwise it
    was refused when a phase skipped with Massive's refusal (its reason starts `REFUSED`), and
    skipped when a phase skipped otherwise, other than the call of such a check that failed as
    expected (pytest reports it as a skip). Otherwise a check shows the problem (`problem
    present`) when its call failed as expected, and its fix (`problem fixed`) when its call
    passed against that expectation, so a setup or teardown that failed or skipped is never
    read as either. Otherwise a test passed when its call passed. A test with no call record
    and no failure did not finish: its child was stopped or died.

    Args:
        phases: The test's records, in order.

    Returns:
        `(status, reason)`: `failed` (with each failed phase), `refused` or `skipped` (with
        its reason), `problem present` (with Massive's values) or `problem fixed`, `passed`,
        or `error` (`"did not finish"`).
    """
    records = list(phases)
    # A line written before the checks of Massive's problems existed has no `problem`.
    failed = [r for r in records if r["outcome"] == "failed" and r.get("problem", "") != FIXED]
    if failed:
        return "failed", ", ".join(f"{r['phase']}: {r['error_type'] or 'failed'}" for r in failed)
    skipped = [r for r in records if r["outcome"] == "skipped" and r.get("problem", "") != PRESENT]
    if skipped:
        reason = str(skipped[0]["reason"])
        return ("refused" if reason.startswith(REFUSED) else "skipped"), reason
    for record in records:
        problem = record.get("problem", "")
        if problem == PRESENT:
            return "problem present", str(record["reason"])
        if problem == FIXED:
            return "problem fixed", PROBLEM_FIXED_REASON
    if any(r["phase"] == "call" and r["outcome"] == "passed" for r in records):
        return "passed", ""
    return "error", "did not finish"
