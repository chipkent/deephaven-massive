"""The user pages' Python examples: reading them from a page, and running them as the console does.

The README's examples are real code, run twice: by `tests/test_docs.py` against the fake
servers in every unit run, and by `tests/live/test_doc_examples_integration.py` against
Massive. Both read each example from the page at test time, so a test never holds a copy that
could drift from what a reader pastes:

- `blocks`, `section_blocks`, and `intro_blocks` read a page's Python blocks, each a `Block`; a
  block fenced as `python continues` is the second step of an example, run in the namespace of
  the block before it, and one fenced as `python skip-test` never runs.
- `settings_commands` reads the README's shell commands that save the settings file, one block
  for the delayed plans and one for the real-time plans, and `write_settings_file` runs them.
- `run_blocks` runs blocks as scripts, each as one console command, and `merged` and `forget`
  read and then drop the names they defined.
- `recorded_sessions` records every session the examples open, and closes them afterwards.
- `dashboard_panels` reads the titles and contents of a `deephaven.ui` dashboard's panels.

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
from typing import TYPE_CHECKING, Any, Final, NamedTuple

import deephaven_massive as dm

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Sequence
    from pathlib import Path

_FENCE: Final = re.compile(r"^```python([^\n]*)\n(.*?)^```", re.DOTALL | re.MULTILINE)
_BASH: Final = re.compile(r"^```bash\n(.*?)^```", re.DOTALL | re.MULTILINE)

SETTINGS_ANCHOR: Final = "what-your-plan-allows"
"""The README section whose shell commands save the settings file the paid examples load."""

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


def settings_commands(page: Path, *, delayed: bool) -> str:
    """Return the README's shell commands that save the settings file for a kind of plan.

    The section has one block of commands per kind of plan: first Starter and Developer's,
    which accept the delayed feed, then Advanced and Business's, which are real-time.

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
    start: dict[str, Any] | None = None,
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
        start: The names a first block that continues an earlier one starts from, such as the
            namespace of an example run by an earlier call; None for none.

    Returns:
        The names each block defined, one namespace per block, in order; a continuing block's
        namespace holds the names it started from too.
    """
    namespaces: list[dict[str, Any]] = []
    for number, block in enumerate(code_blocks):
        script = tmp_path / f"{name}_{number}.py"
        script.write_text(block.code, encoding="utf-8")
        before = (namespaces[-1] if namespaces else start) if block.continues else None

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
def recorded_sessions() -> Iterator[list[dm.MassiveSession]]:
    """Record every session `dm.MassiveSession(...)` opens, and close each one at the end.

    The examples open their sessions as a reader does, through `dm.MassiveSession`; inside the
    block, that name makes a real session and records it. The package resolves the name
    lazily, so the stand-in is bound in the package only for the block, then removed.

    Yields:
        The sessions opened so far, in order; the list grows as the examples open more.
    """
    real = dm.MassiveSession
    opened: list[dm.MassiveSession] = []

    def session(config: dm.MassiveConfig | None = None) -> dm.MassiveSession:
        made = real(config)
        opened.append(made)
        return made

    vars(dm)["MassiveSession"] = session
    try:
        yield opened
    finally:
        vars(dm).pop("MassiveSession", None)
        for made in opened:
            made.close()


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
