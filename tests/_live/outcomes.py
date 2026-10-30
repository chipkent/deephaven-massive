"""The live run's record of each test phase's outcome, written as the tests run.

The live-run program (`live_run`) runs the live suites in child pytest processes, loading this
module as a plugin (`-p tests._live.outcomes`). For every phase of every test (setup, call,
teardown) the plugin appends one JSON line to the file `MASSIVE_LIVE_OUTCOMES` names, and
flushes it, so the record survives a child that is killed, crashes, or ends the JVM: pytest's
own reports would be lost with the process. Each line holds:

- `nodeid`, `phase` (`setup`, `call`, or `teardown`), and `outcome` (`passed`, `failed`, or
  `skipped`);
- `reason`, a skip's reason without pytest's `Skipped: ` prefix, else `""`;
- `error_type`, the class name of a failure's exception (such as `EntitlementError`), else `""`;
- `items`, the design's open questions the test answers (its `settles` and `verifies`
  markers), collected once per run, since a report carries no markers.

At collection, with `MASSIVE_LIVE_COLLECTION` set, the plugin also writes that file: a JSON
object of every collected test's items and declared REST requests (its `requests` marker, 10
when absent), from which the program lists a group's checks and estimates its duration. The
plugin adds the option `--live-run-token`, which only marks the child's command line so the
program can tell it from another process (`children.is_alive`).

Without either variable the plugin writes nothing. `read_outcomes` reads the file back
for the program, which turns each test's phases into one check status (`check_outcome`).
"""

from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final

import pytest

if TYPE_CHECKING:
    from collections.abc import Generator, Iterable

OUTCOMES_VARIABLE: Final = "MASSIVE_LIVE_OUTCOMES"
"""The environment variable that names the file the plugin appends to."""

COLLECTION_VARIABLE: Final = "MASSIVE_LIVE_COLLECTION"
"""The environment variable that names the file the plugin writes the collected tests to."""

DEFAULT_REQUESTS: Final = 10
"""The REST requests a test is assumed to make when it declares none."""

_ITEMS: dict[str, list[str]] = {}
"""Each collected test's open-question items, by node id."""

_LOCK = threading.Lock()
"""Serializes the appends, since a report may come from a timeout thread."""

REFUSALS: Final = frozenset({"EntitlementError", "WireEntitlementError"})
"""The exception types of a refusal by Massive: a failure with one is a refusal of an allowed
test, which the program re-probes before it calls it a failure."""


def pytest_addoption(parser: pytest.Parser) -> None:
    """Add `--live-run-token`, which marks a live-run child's command line.

    Args:
        parser: pytest's option parser.
    """
    parser.addoption(
        "--live-run-token",
        default="",
        help="a random token the live-run program puts on its child's command line, to "
        "recognize the child later; it changes nothing in the run",
    )


@pytest.hookimpl(trylast=True)
def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Keep each test's items, and write the collection when the program asks for it.

    Runs last, after `tests/conftest.py` has deselected the tiers the run leaves out.

    Args:
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
        requests = item.get_closest_marker("requests")
        collection[item.nodeid] = {
            "items": _ITEMS[item.nodeid],
            "requests": int(requests.args[0]) if requests is not None else DEFAULT_REQUESTS,
        }
    target = os.environ.get(COLLECTION_VARIABLE, "")
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
    target = os.environ.get(OUTCOMES_VARIABLE, "")
    if target:
        error_type = call.excinfo.typename if report.failed and call.excinfo is not None else ""
        line = {
            "nodeid": item.nodeid,
            "phase": report.when,
            "outcome": report.outcome,
            "reason": skip_reason(report.longrepr) if report.skipped else "",
            "error_type": error_type,
            "items": _ITEMS.get(item.nodeid, []),
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

    A test passed when its call passed and no phase failed; it was skipped when a phase was
    skipped and none failed; it failed when any phase failed, and was refused when that failure
    was Massive's refusal (`REFUSALS`). A test with no call record and no failure did not
    finish: its child was stopped or died.

    Args:
        phases: The test's records, in order.

    Returns:
        `(status, reason)`: `passed`, `failed`, `refused`, `skipped` (with its reason), or
        `error` (`"did not finish"`).
    """
    records = list(phases)
    failed = [r for r in records if r["outcome"] == "failed"]
    if failed:
        refused = all(r["error_type"] in REFUSALS for r in failed)
        kinds = ", ".join(f"{r['phase']}: {r['error_type'] or 'failed'}" for r in failed)
        return ("refused" if refused else "failed"), kinds
    skipped = [r for r in records if r["outcome"] == "skipped"]
    if skipped:
        return "skipped", str(skipped[0]["reason"])
    if any(r["phase"] == "call" and r["outcome"] == "passed" for r in records):
        return "passed", ""
    return "error", "did not finish"
