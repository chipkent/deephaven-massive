"""Running the tester guide's examples in one go, as the web console would, and saving them.

`docs/tester-guide.md` shows each exercise twice: as a code block to paste into the
Deephaven web console, and as a file of `docs/tester-examples/`, such as
`01_start_a_session.py`, holding the same code (`guide_blocks` reads the blocks, and a unit
test checks that each matches its file). `run_examples` runs the files in order in one shared
namespace, so a later example sees the variables of the earlier ones, exactly as the
console keeps them. Each example runs under the update graph's exclusive lock, as every
console command does, so the examples meet the library under the same constraint the tester
does. Before each example it waits, without the lock, until no background download of an
open session is still `LOADING`, as the guide tells the tester to.

For each example it writes, under the output folder:

- `<example>.txt`: what the example printed, then its error, if any;
- `tables/<example>/<variable>.csv`: the first `TABLE_ROWS` rows of each table the example
  created or replaced;
- `summary.txt`: one line per example, with its outcome and how long it took.

Every text it writes passes through the secret filter (`SecretFilter.clean`). An example
ends `passed`; `refused by your plan` when it raised `EntitlementError`, a refusal by
Massive because of the key's plans, which is expected on a free key; or `failed` with any
other error, which is a finding to report. Every table the examples make belongs to one
liveness scope; at the end the runner closes every session the examples left open, forgets
their variables, and releases that scope, so nothing they started outlives the run.

This module imports Deephaven, so it is imported only once a Deephaven server runs.
"""

from __future__ import annotations

import contextlib
import io
import re
import runpy
import time
import traceback
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final

from deephaven.execution_context import get_exec_ctx
from deephaven.pandas import to_pandas
from deephaven.table import Table

import deephaven_massive as dm
from deephaven_massive._tables.liveness import new_scope, release_scope

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from contextlib import AbstractContextManager
    from pathlib import Path

    from tests._live.live_run.secret_scrub import SecretFilter

EXAMPLE_NAME: Final = re.compile(r"^\d\d_[a-z0-9_]+\.py$")
"""The name of an example file: two digits that give its order, then what it shows."""

GUIDE_HEADING: Final = re.compile(r"^#{2,4} .*\(`(\d\d_[a-z0-9_]+\.py)`\)\s*$")
"""A guide heading that names an example's file, such as `### 3. Daily bars
(`03_daily_bars.py`)`; the next Python code block under it is the example."""

PASSED: Final = "passed"
REFUSED: Final = "refused by your plan"
FAILED: Final = "failed"

TABLE_ROWS: Final = 1000
"""How many rows of each table an example made are saved."""

DOWNLOAD_TIMEOUT: Final = 900.0
"""Seconds to wait for background downloads before the next example runs anyway: their 10
minutes of rate-limit pauses, and some time to read."""

POLL_INTERVAL: Final = 1.0
"""Seconds between looks at the sessions' downloads."""


@dataclass(frozen=True)
class Outcome:
    """How one example ended.

    Attributes:
        name: The example's file name.
        status: `PASSED`, `REFUSED`, or `FAILED`.
        seconds: How long it ran.
        error: The error's type and message for a refusal; for a failure, its type and the
            first line of its message, then its traceback; or `""`.
    """

    name: str
    status: str
    seconds: float
    error: str


def example_files(folder: Path) -> list[Path]:
    """List a folder's example files in the order they run.

    Args:
        folder: The examples folder, `docs/tester-examples/`.

    Returns:
        The files whose names match `EXAMPLE_NAME`, sorted by name.
    """
    return sorted(p for p in folder.iterdir() if EXAMPLE_NAME.match(p.name))


def guide_blocks(text: str) -> dict[str, str]:
    """Read the guide's example blocks: each heading that names a file, and its code.

    Args:
        text: The guide, `docs/tester-guide.md`.

    Returns:
        Each named file and the code of the first Python block after its heading, ending
        with a newline, as the file does.
    """
    blocks: dict[str, str] = {}
    name = ""
    code: list[str] | None = None
    for line in text.splitlines():
        if code is not None:
            if line.rstrip() == "```":
                blocks[name] = "\n".join(code) + "\n"
                name, code = "", None
            else:
                code.append(line)
        elif heading := GUIDE_HEADING.match(line):
            name = heading.group(1)
        elif name and line.rstrip() == "```python":
            code = []
    return blocks


@contextlib.contextmanager
def console_lock() -> Iterator[None]:
    """Hold the update graph's exclusive lock, which every console command holds.

    It takes the Java lock itself: Deephaven's own `exclusive_lock` would wrap an example's
    error in a `DHError`, hiding a refusal's `EntitlementError`.

    Yields:
        Control, holding the exclusive lock of the calling thread's execution context's
        update graph.
    """
    lock = get_exec_ctx().update_graph.j_update_graph.exclusiveLock()
    lock.lock()
    try:
        yield
    finally:
        lock.unlock()


def sessions(namespace: dict[str, Any]) -> list[dm.MassiveSession]:
    """Find the sessions among a namespace's variables.

    Args:
        namespace: The examples' shared variables.

    Returns:
        Every `MassiveSession` value, once each.
    """
    found: dict[int, dm.MassiveSession] = {}
    for value in namespace.values():
        if isinstance(value, dm.MassiveSession):
            found.setdefault(id(value), value)
    return list(found.values())


def loading(session: dm.MassiveSession) -> int:
    """Count a session's background downloads still running.

    Args:
        session: The session.

    Returns:
        The rows of `streams()` whose `State` is `LOADING`; 0 for a closed session.
    """
    try:
        frame = to_pandas(session.streams().snapshot())
    except dm.MassiveError:  # a closed session has no downloads left
        return 0
    return int((frame["State"] == "LOADING").sum())


def wait_for_downloads(
    namespace: dict[str, Any],
    *,
    timeout: float = DOWNLOAD_TIMEOUT,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> bool:
    """Wait until no session of the namespace has a background download running.

    Args:
        namespace: The examples' shared variables.
        timeout: The most seconds to wait.
        clock: Reads the time, in seconds.
        sleep: Waits a number of seconds.

    Returns:
        True once nothing is loading; False when the time ran out first.
    """
    deadline = clock() + timeout
    while any(loading(session) for session in sessions(namespace)):
        if clock() >= deadline:
            return False
        sleep(POLL_INTERVAL)
    return True


def run_example(
    path: Path,
    namespace: dict[str, Any],
    *,
    lock: Callable[[], AbstractContextManager[None]] = console_lock,
    clock: Callable[[], float] = time.monotonic,
) -> tuple[Outcome, str]:
    """Run one example under the lock, in the shared namespace, capturing what it prints.

    Args:
        path: The example's file.
        namespace: The examples' shared variables; updated with the example's own when it
            completes, and left as it was when it raises.
        lock: Makes the lock every console command holds.
        clock: Reads the time, in seconds.

    Returns:
        The outcome, and what the example printed.
    """
    printed = io.StringIO()
    start = clock()
    status, error = PASSED, ""
    try:
        with lock(), contextlib.redirect_stdout(printed):
            namespace.update(runpy.run_path(str(path), init_globals=namespace))
    except dm.EntitlementError as refusal:
        status, error = REFUSED, f"{type(refusal).__name__}: {refusal}"
    except Exception as failure:  # noqa: BLE001 - any other error of an example is a finding
        headline = f"{type(failure).__name__}: {str(failure).strip().partition(chr(10))[0]}"
        status, error = FAILED, f"{headline}\n{traceback.format_exc()}"
    return Outcome(path.name, status, round(clock() - start, 1), error), printed.getvalue()


def tables_of(namespace: dict[str, Any]) -> dict[str, Table]:
    """Return a namespace's table variables.

    Args:
        namespace: The examples' shared variables.

    Returns:
        Each variable that holds a Deephaven table, by name; private names left out.
    """
    return {n: v for n, v in namespace.items() if isinstance(v, Table) and not n.startswith("_")}


def save_tables(tables: dict[str, Table], folder: Path, secrets: SecretFilter) -> None:
    """Save the first rows of each table as a CSV file, cleaned of the secrets.

    Args:
        tables: The tables, by variable name.
        folder: Where the files go, created if missing.
        secrets: The secrets to keep out of the files.
    """
    for name, table in tables.items():
        folder.mkdir(parents=True, exist_ok=True)
        text = to_pandas(table.head(TABLE_ROWS).snapshot()).to_csv(index=False)
        (folder / f"{name}.csv").write_text(secrets.clean(text), encoding="utf-8")


@contextlib.contextmanager
def console_scope(namespace: dict[str, Any]) -> Iterator[None]:
    """Own every table the examples make, and free it all on the way out, whatever happened.

    The console keeps a table until its variable goes; here one liveness scope, open for the
    whole run, holds every table the examples make. On the way out it closes every session
    of the namespace, empties the namespace, and releases the scope while holding the update
    graph's shared lock, so no refreshing table outlives the run.

    Args:
        namespace: The examples' shared variables; emptied at the end.

    Yields:
        Control, with the scope open on this thread.
    """
    scope = new_scope()
    try:
        with scope.open():
            yield
    finally:
        for session in sessions(namespace):
            session.close()
        namespace.clear()
        release_scope(get_exec_ctx(), scope)


def run_examples(
    files: list[Path],
    out: Path,
    *,
    secrets: SecretFilter,
    echo: Callable[[str], None],
    lock: Callable[[], AbstractContextManager[None]] = console_lock,
    wait: Callable[[dict[str, Any]], bool] = wait_for_downloads,
) -> list[Outcome]:
    """Run the examples in order, as one console session, and save what each did.

    Args:
        files: The example files, in order (`example_files`).
        out: The output folder, created if missing.
        secrets: The secrets to keep out of every file written and line echoed.
        echo: Prints one line of progress.
        lock: Makes the lock every console command holds.
        wait: Waits for the background downloads before each example; returns False when it
            gave up.

    Returns:
        Each example's outcome.
    """
    out.mkdir(parents=True, exist_ok=True)
    namespace: dict[str, Any] = {}
    outcomes: list[Outcome] = []
    with console_scope(namespace):
        for path in files:
            if not wait(namespace):
                echo("a background download is still running; going on without it")
            before = dict(tables_of(namespace))
            echo(f"{path.name}: running")
            outcome, printed = run_example(path, namespace, lock=lock)
            outcomes.append(outcome)
            text = printed + (f"\n{outcome.error}" if outcome.error else "")
            (out / f"{path.stem}.txt").write_text(secrets.clean(text), encoding="utf-8")
            made = {n: t for n, t in tables_of(namespace).items() if before.get(n) is not t}
            save_tables(made, out / "tables" / path.stem, secrets)
            echo(secrets.clean(summary_line(outcome)))
    lines = [summary_line(o) for o in outcomes]
    (out / "summary.txt").write_text(secrets.clean("\n".join(lines)) + "\n", encoding="utf-8")
    return outcomes


def summary_line(outcome: Outcome) -> str:
    """Describe an outcome in one line.

    Args:
        outcome: The outcome.

    Returns:
        Such as `"03_daily_bars.py: passed (2.1 s)"`, with the first line of a refusal or a
        failure after it.
    """
    line = f"{outcome.name}: {outcome.status} ({outcome.seconds:g} s)"
    if outcome.error:
        line += " - " + outcome.error.strip().splitlines()[0]
    return line
