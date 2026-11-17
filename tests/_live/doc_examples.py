"""The user pages' Python examples: reading them from a page, and running them as the console does.

The README's examples are real code, run twice: by `tests/test_docs.py` against the fake
servers in every unit run, and by `tests/live/test_doc_examples_integration.py` against
Massive. Both read each example from the page at test time, so a test never holds a copy that
could drift from what a reader pastes:

- `blocks`, `section_blocks`, and `intro_blocks` read a page's Python blocks, each a `Block`; a
  block fenced as `python continues` is the second step of an example, run in the namespace of
  the block before it, and one fenced as `python skip-test` never runs. `examples` groups a
  page's blocks into its examples, each a block with the blocks that continue it.
- `settings_commands` reads the README's shell commands that save the settings file, one block
  for the delayed plans and one for the real-time plans, and `write_settings_file` runs them.
- `run_blocks` runs blocks as scripts, each as one console command, and `merged` and `forget`
  read and then drop the names they defined; `Console` runs them in one reader's console,
  where a later example rebinds the names of an earlier one.
- `recorded_sessions` closes every session the examples opened, once they are done.
- `HISTORY_SECTIONS`, `LIVE_SECTIONS`, and `UNRUN_SECTIONS` name the README's examples the live run
  runs, and those it leaves out.
- `dashboard_panels` reads the titles and contents of a `deephaven.ui` dashboard's panels.
- `snapshots_cleared` and `seam_gaps` are the pure rules the live checks apply to Example 7's
  snapshot and Example 11's backfill.

This module imports the library, so it is imported only once a Deephaven server runs.
"""

from __future__ import annotations

import contextlib
import os
import re
import runpy
import shutil
import subprocess
import types
import weakref
from datetime import datetime, timedelta
from datetime import time as dt_time
from typing import TYPE_CHECKING, Any, Final, NamedTuple

import deephaven_massive as dm
from tests._live.calendar import NEW_YORK

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator, Sequence
    from pathlib import Path

    from deephaven_massive._tables.session_core import SessionCore

_FENCE: Final = re.compile(r"^```python([^\n]*)\n(.*?)^```", re.DOTALL | re.MULTILINE)
_BASH: Final = re.compile(r"^```bash\n(.*?)^```", re.DOTALL | re.MULTILINE)
_ANCHOR: Final = re.compile(r'<a id="([a-z0-9-]+)"></a>')

SETTINGS_ANCHOR: Final = "what-your-plan-allows"
"""The README section whose shell commands save the settings file the live examples load."""

HISTORY_SECTIONS: Final = (
    "read-daily-bars",
    "whole-market-day",
    "compare-stocks",
    "chart-closes",
    "follow-the-news",
    "stop-live-tables",
    "drive-with-a-watchlist",
    "convert-symbols",
)
"""The README's examples that read stock history with no setting, which follow its hook:
Examples 1 to 6, the conversion of Yahoo tickers that ends Example 6, and Example 13."""

LIVE_SECTIONS: Final = (
    "what-your-plan-allows",
    "stream-minute-bars",
    "change-since-close",
    "table-shapes",
    "backfill-then-live",
    "dashboard",
)
"""The README's examples that load the settings file and stream stocks, 7 to 12, in the
README's order."""

UNRUN_SECTIONS: Final = {
    "massive-config": "it opens a session and reads nothing; tests/test_docs.py runs it",
    "bad-symbols-and-rows": (
        "it sets error policies, which tests/test_docs.py checks; its one read is Example 1's "
        "daily bars"
    ),
}
"""The README sections with Python blocks that the live run of the examples leaves out, and why."""

_DASHBOARD: Final = "deephaven.ui.components.Dashboard"
_PANEL: Final = "deephaven.ui.components.Panel"


class Block(NamedTuple):
    """One Python block of a user page that runs."""

    code: str
    """The block's code."""
    continues: bool
    """Whether it runs in the namespace of the block before it (a `python continues` fence)."""


def fences(text: str) -> list[tuple[str, str]]:
    """Return the info string after `python` and the code of each Python block, in order.

    Args:
        text: A Markdown page.

    Returns:
        One `(info, code)` pair per block; `info` is `"continues"` for an example's second step.
    """
    return [(info.strip(), code) for info, code in _FENCE.findall(text)]


def blocks(text: str) -> list[Block]:
    """Return each Python block of a Markdown text that runs, in order.

    Args:
        text: A Markdown page, or a part of one.

    Returns:
        The blocks, leaving out those fenced as `python skip-test`.
    """
    return [Block(code, info == "continues") for info, code in fences(text) if info != "skip-test"]


def section(page: Path, anchor: str) -> str:
    """Return the text of one section of a page, from its anchor to the next anchor.

    Args:
        page: The user page.
        anchor: The section's `<a id>`, such as `"whole-market-day"`.

    Returns:
        The section's Markdown.
    """
    text = page.read_text(encoding="utf-8")
    start = text.index(f'<a id="{anchor}"></a>')
    end = text.find("<a id=", start + 1)
    return text[start : end if end >= 0 else len(text)]


def section_blocks(page: Path, anchor: str) -> list[Block]:
    """Return the Python blocks of one section of a page, in order (see `section`).

    Args:
        page: The user page.
        anchor: The section's `<a id>`.

    Returns:
        The blocks that run.
    """
    return blocks(section(page, anchor))


def intro_blocks(page: Path) -> list[Block]:
    """Return the Python blocks of a page before its first anchor, such as the README's hook.

    Args:
        page: The user page.

    Returns:
        The blocks that run.
    """
    text = page.read_text(encoding="utf-8")
    end = text.find("<a id=")
    return blocks(text[: end if end >= 0 else len(text)])


class Example(NamedTuple):
    """One example of a user page: a block, and the blocks that continue it, in order."""

    anchor: str
    """The `<a id>` of the section the example starts in; `""` before the page's first anchor."""
    blocks: list[Block]
    """The example's blocks: the first starts afresh, and each later one continues it."""


def examples(page: Path) -> list[Example]:
    """Return the examples of a page, in order, each with the anchor of its section.

    Every block that runs belongs to exactly one example: a block that continues the one before
    it joins that block's example, even across a section's end, and every other block starts an
    example.

    Args:
        page: The user page.

    Returns:
        The examples.
    """
    parts = _ANCHOR.split(page.read_text(encoding="utf-8"))
    sections = zip(["", *parts[1::2]], parts[::2], strict=True)
    found: list[Example] = []
    for anchor, text in sections:
        for block in blocks(text):
            if block.continues and found:
                found[-1].blocks.append(block)
            else:
                found.append(Example(anchor, [block]))
    return found


def settings_commands(page: Path, *, delayed: bool) -> str:
    """Return the README's shell commands that save the settings file for a kind of plan.

    The section has two blocks of commands: first the one for a key Massive serves stocks
    delayed, which lists them in `delayed_subscriptions`, then the one for real-time data.

    Args:
        page: The README.
        delayed: True for the delayed plans' commands, False for the real-time plans'.

    Returns:
        The commands.
    """
    found: list[str] = _BASH.findall(section(page, SETTINGS_ANCHOR))
    delayed_commands, real_time = found
    assert "delayed_subscriptions: []" in real_time
    return delayed_commands if delayed else real_time


def write_settings_file(page: Path, home: Path, *, delayed: bool) -> None:
    """Run the README's commands that save the settings file, with `HOME` at `home`.

    Args:
        page: The README.
        home: The folder that stands in for the reader's home folder.
        delayed: True to run the delayed plans' commands, False the real-time plans'.
    """
    bash = shutil.which("bash")
    assert bash is not None, "the README's shell commands need bash"
    subprocess.run(  # noqa: S603 - bash running the README's own commands, no shell string
        [bash, "-c", settings_commands(page, delayed=delayed)],
        check=True,
        env={**os.environ, "HOME": str(home)},
    )


def run_blocks(
    code_blocks: Sequence[Block],
    tmp_path: Path,
    name: str,
    *,
    console_command: Callable[[Callable[[], None]], None],
) -> list[dict[str, Any]]:
    """Run code blocks as scripts, in order, as a reader pastes them into the console.

    A block runs on its own, as in a fresh console, unless it continues the block before it:
    then it starts from that block's names, as in the same console. Each block runs as one
    console command (the `console_command` fixture): under the update graph's exclusive lock,
    as a reader pasting it into the Deephaven console runs it. A block that creates an input
    table and adds a row at once, as the watchlist examples do, is then never interrupted by an
    update cycle; without the lock, a cycle running while the table is created can refresh it
    in the step it was stamped with, and Deephaven exits the process.

    Args:
        code_blocks: The blocks, in order.
        tmp_path: Where to write the scripts.
        name: The scripts' file name prefix.
        console_command: Runs one block the way the console runs a command.

    Returns:
        The names each block defined, one namespace per block, in order; a continuing block's
        namespace holds the names it started from too.
    """
    namespaces: list[dict[str, Any]] = []
    for number, block in enumerate(code_blocks):
        script = tmp_path / f"{name}_{number}.py"
        script.write_text(block.code, encoding="utf-8")
        before = namespaces[-1] if block.continues and namespaces else None

        def run(script: Path = script, before: dict[str, Any] | None = before) -> None:
            namespaces.append(runpy.run_path(str(script), init_globals=before))

        console_command(run)
    return namespaces


def merged(namespaces: Sequence[dict[str, Any]]) -> dict[str, Any]:
    """Return the names of several blocks as one mapping, a later block's name winning.

    Args:
        namespaces: The names each block defined, as `run_blocks` returns them.

    Returns:
        Every name, bound to the value its last defining block gave it.
    """
    names: dict[str, Any] = {}
    for namespace in namespaces:
        names.update(namespace)
    return names


def forget(namespaces: Sequence[dict[str, Any]]) -> None:
    """Break the cycle between the scripts' functions and their globals, then drop the names.

    A function a script defined holds its module globals, which hold the function and the
    session, so the session goes with the test only once those globals are cleared.

    Args:
        namespaces: The names each script defined.
    """
    for names in namespaces:
        for value in names.values():
            if isinstance(value, types.FunctionType) and value.__module__ == "<run_path>":
                value.__globals__.clear()
        names.clear()


@contextlib.contextmanager
def recorded_sessions() -> Iterator[None]:
    """Close every session `dm.MassiveSession(...)` opens inside the block, at its end.

    The examples open their sessions as a reader does, through `dm.MassiveSession`; inside the
    block, that name makes a real session and remembers it weakly, so a session the examples
    no longer reach can still be collected, as in a reader's console. The stand-in is bound in
    the package only for the block, and the real class is bound again at its end.

    Yields:
        Nothing; the examples run in between.
    """
    real = dm.MassiveSession
    opened: list[weakref.ref[SessionCore]] = []

    def session(config: dm.MassiveConfig | None = None) -> dm.MassiveSession:
        made = real(config)
        opened.append(weakref.ref(made._core))
        return made

    vars(dm)["MassiveSession"] = session
    try:
        yield
    finally:
        vars(dm)["MassiveSession"] = real
        for core in (ref() for ref in opened):
            if core is not None:
                core.close()


class Console:
    """One reader's console: every block runs in its names, so a later example rebinds `m`.

    Each block runs as one console command (the `console_command` fixture), under the update
    graph's exclusive lock, as the Deephaven console runs a paste. The names a block binds
    replace the console's, as a paste's do; a name no later block rebinds stays.
    """

    def __init__(self, tmp_path: Path, command: Callable[[Callable[[], None]], None]) -> None:
        """Open an empty console.

        Args:
            tmp_path: Where to write each block's script.
            command: Runs one block the way the console runs a command.
        """
        self._tmp_path = tmp_path
        self._command = command
        self._names: dict[str, Any] = {}
        self._count = 0

    def run(self, code_blocks: Sequence[Block], *keep: str) -> dict[str, Any]:
        """Run blocks in order, as pasted one after the other, and return the names asked for.

        Args:
            code_blocks: The blocks.
            keep: The names to return, as the last block left them.

        Returns:
            Each name in `keep`, bound to its value.
        """
        for block in code_blocks:
            script = self._tmp_path / f"console_{self._count}.py"
            self._count += 1
            script.write_text(block.code, encoding="utf-8")

            def paste(script: Path = script) -> None:
                self._names.update(runpy.run_path(str(script), init_globals=self._names))

            self._command(paste)
        return {name: self._names[name] for name in keep}

    def close(self) -> None:
        """Forget every name, as closing the console does."""
        forget([self._names])


def dashboard_panels(dashboard: object) -> list[tuple[str, object]]:
    """Return the title and the content of each panel of a `deephaven.ui` dashboard, in order.

    A dashboard holds one row or column of panels, perhaps nested in further rows and columns;
    each panel holds one table or chart.

    Args:
        dashboard: The result of `ui.dashboard(...)`.

    Returns:
        One `(title, content)` pair per panel; empty when `dashboard` is not a dashboard.
    """
    if getattr(dashboard, "name", None) != _DASHBOARD:
        return []
    found: list[tuple[str, object]] = []

    def visit(element: object) -> None:
        props = element.render() if hasattr(element, "render") else {}
        if getattr(element, "name", None) == _PANEL:
            found.append((str(props.get("title")), props.get("children")))
            return
        children = props.get("children")
        for child in children if isinstance(children, list) else [children]:
            if child is not None:
                visit(child)

    visit(dashboard)
    return found


SNAPSHOTS_CLEARED: Final = (dt_time(3, 30), dt_time(4, 0))
"""When Massive's stock snapshots are empty each day, in New York time: cleared at 03:30 and
refilled from about 04:00 (`vendor/massive/rest/stocks/snapshots/`)."""


def snapshots_cleared(now: datetime, delay: timedelta) -> bool:
    """Tell whether a plan's stock snapshots are in their daily empty window.

    Args:
        now: An aware instant.
        delay: The plan's feed delay, zero on a real-time plan.

    Returns:
        True from 03:30 New York time, when Massive clears the snapshots whatever the plan, until
        04:00 plus the delay, when a delayed plan's first refilled data is due
        (`SNAPSHOTS_CLEARED`).
    """
    wall = now.astimezone(NEW_YORK)
    cleared = datetime.combine(wall.date(), SNAPSHOTS_CLEARED[0], NEW_YORK)
    refilled = datetime.combine(wall.date(), SNAPSHOTS_CLEARED[1], NEW_YORK) + delay
    return cleared <= wall < refilled


def seam_gaps(
    spy: Iterable[int], live: Iterable[int], rest: Iterable[int]
) -> tuple[list[int], list[int]]:
    """Compare a backfilled table's minutes with REST's, through its last live minute.

    The README's backfill example merges the day's history with the live bars; where the two
    meet, a minute could go missing. Only minutes up to the last live one are compared, since
    REST may already hold a later minute the stream has not sent yet.

    Args:
        spy: The start of each minute the merged table holds, as epoch nanoseconds.
        live: The start of each minute the stream delivered.
        rest: The start of each minute REST holds of the day.

    Returns:
        The minutes REST holds that the table lacks, then those the table holds that REST
        lacks, each sorted; both empty when no live minute arrived.
    """
    last = max(live, default=None)
    if last is None:
        return [], []
    held = {start for start in spy if start <= last}
    known = {start for start in rest if start <= last}
    return sorted(known - held), sorted(held - known)
