"""Running the tester guide's examples in one go, as the web console would, and saving them.

`docs/tester-guide.md` shows each exercise twice: as a code block to paste into the
Deephaven web console, and as a file of `docs/tester-examples/`, such as
`01_start_a_session.py`, holding the same code (`guide_blocks` reads the blocks by the
example's number, and a unit test checks that each matches its file). Each example opens its
own session, so it runs as pasted into a fresh console. `run_examples` runs the files in order
in one shared namespace, as the console keeps them, so the diagnostics example finds the
session of the example before it in `m`. Each example runs under the update graph's exclusive
lock, as every console command does, so the examples meet the library under the same
constraint the tester does. Before each example it waits, without the lock, until no
background download of an open session is still `LOADING`.

For each example it writes, under the output folder:

- `<example>.txt`: what the example printed, then its error, if any;
- `tables/<example>/<variable>.csv`: the first `TABLE_ROWS` rows of each table the example
  created or replaced, or `<variable>.error.txt` for a table that could not be read, such as
  a refreshing table that failed after its example returned, which the example's text and
  the progress lines also name;
- `summary.txt`: one line per example, with its outcome and how long it took.

Every text it writes passes through the secret filter (`SecretFilter.clean`). An example ends
`passed`; `refused by Massive` when it raised Massive's `EntitlementError`, one whose `source` is
`ErrorSource.MASSIVE` and that carries Massive's `status`, which Massive sends for data the key may
not read; `not listed as delayed` when it raised the `EntitlementError` of the library's settings
(its `source` is `ErrorSource.USER`: Massive labels the data delayed and the settings file does not
list its subscription in `delayed_subscriptions`); or `failed` with any other error, which is a
finding to report. A stream that Massive refuses fails after its example returns, and so does a
`fetch()` or `history()` that reads in the background, so after the last example the run waits
`REFUSAL_WAIT` seconds and gives a passed example the verdict of its sessions' failed streams and
reads (`stream_verdict`). Every table the examples make belongs to one liveness scope; at the end
the runner closes every session the examples opened, forgets their variables, and releases that
scope, so nothing they started outlives the run.

Most examples open their session with `dm.MassiveSession()`, as the README's do. The live
Stocks examples load the settings file the README's settings example saves, `SETTINGS_PATH`;
`run_examples` writes a settings file listing the subscriptions the tester's own settings list
as delayed (`listed_delayed`, `settings_text`) and runs a copy of each such example that reads
it instead (`fitted`), so the run writes nothing into the tester's own file.

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
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final

from deephaven.execution_context import get_exec_ctx
from deephaven.pandas import to_pandas
from deephaven.table import Table

import deephaven_massive as dm
from deephaven_massive._config import CONFIG_FILE_VARIABLE
from deephaven_massive._tables.liveness import new_scope, release_scope
from tests._live.refusals import is_massive_refusal

if TYPE_CHECKING:
    from collections.abc import Callable, Collection, Iterator, Mapping
    from contextlib import AbstractContextManager

    from tests._live.live_run.secret_scrub import SecretFilter

EXAMPLE_NAME: Final = re.compile(r"^\d\d_[a-z0-9_]+\.py$")
"""The name of an example file: two digits that give its order, then what it shows."""

GUIDE_HEADING: Final = re.compile(r"^#### (\d+)\. ")
"""A guide heading that numbers an example, such as `#### 3. Download two weeks of daily
bars`; the first Python code block under it, before the next heading, is the example whose
file name starts with that number."""

SESSION_LINE: Final = re.compile(r"^m = dm\.MassiveSession\(.*\)\n", re.MULTILINE)
"""A line of an example that opens its session in `m`."""

PASSED: Final = "passed"
REFUSED: Final = "refused by Massive"
NOT_LISTED: Final = "not listed as delayed"
FAILED: Final = "failed"

TABLE_ROWS: Final = 1000
"""How many rows of each table an example made are saved."""

DOWNLOAD_TIMEOUT: Final = 900.0
"""Seconds to wait for background downloads before the next example runs anyway: their 10
minutes of rate-limit pauses, and some time to read."""

POLL_INTERVAL: Final = 1.0
"""Seconds between looks at the sessions' downloads."""

REFUSAL_WAIT: Final = 15.0
"""Seconds the run waits after the last example for Massive's answers to the examples'
streams: a refused subscription fails its stream seconds after the example returned."""

STREAM_REFUSAL: Final = "refused the subscription"
"""The words of a stream's `Error` when Massive's WebSocket server refused its subscription;
the connection writes them (`ws_connection`, `_refused`)."""

READ_REFUSAL: Final = "does not include"
"""The words of a background read's `Error` when Massive refused it with `EntitlementError`:
the REST and flat-file refusals say what the key "does not include"
(`RestReader._dataset_error`, `_ingest/flatfiles.py`)."""

DELAYED_REFUSAL: Final = "MassiveConfig.delayed_subscriptions"
"""The words of a stream's or read's `Error` when the settings refused delayed data: the
library's refusal names the setting that accepts it (`recency_labels.delayed_refusal`)."""

SETTINGS_PATH: Final = "~/massive/settings.yaml"
"""The settings file the live examples read, where the README's settings example saves it."""

TEST_FOLDER: Final = "~/massive-test"
"""The tester's folder, where the examples write their debug log and the diagnostics."""


def listed_delayed(environ: Mapping[str, str]) -> tuple[str, ...] | None:
    """Read the subscriptions the tester's own settings file lists as delayed.

    The file is the one `DEEPHAVEN_MASSIVE_CONFIG_FILE` names, else `SETTINGS_PATH`, where
    the README's settings example saves it. Which subscriptions Massive serves delayed is the
    tester's to say, as their dashboard shows it; nothing asks Massive.

    Args:
        environ: The environment.

    Returns:
        The subscriptions, sorted; None when there is no such file.

    Raises:
        ValueError: When the library refuses the file.
    """
    path = Path(environ.get(CONFIG_FILE_VARIABLE, "").strip() or SETTINGS_PATH).expanduser()
    if not path.exists():
        return None
    config = dm.MassiveConfig.from_file(path)
    return tuple(sorted(s.value for s in config.delayed_subscriptions))


def settings_text(delayed: Collection[str]) -> str:
    """Write the settings file the live examples read.

    Args:
        delayed: The subscriptions to list in `delayed_subscriptions`, so their live examples
            read the delayed feed (`listed_delayed`); empty lists none.

    Returns:
        The file's YAML, such as `delayed_subscriptions: [futures, stocks]` and a newline.
    """
    return f"delayed_subscriptions: [{', '.join(sorted(delayed))}]\n"


def fitted(path: Path, *, settings: Path, test_folder: Path, folder: Path) -> Path:
    """Return the example to run: the file itself, or a copy that uses the run's own paths.

    Args:
        path: The example's file.
        settings: The settings file the run wrote (`settings_text`).
        test_folder: The tester's folder of this run, which may not be `TEST_FOLDER`.
        folder: Where a changed copy goes, created if missing.

    Returns:
        `path` when it names neither `SETTINGS_PATH` nor `TEST_FOLDER`; otherwise a copy in
        `folder` that names `settings` and `test_folder` instead.
    """
    code = path.read_text(encoding="utf-8")
    if SETTINGS_PATH not in code and TEST_FOLDER not in code:
        return path
    folder.mkdir(parents=True, exist_ok=True)
    copy = folder / path.name
    code = code.replace(SETTINGS_PATH, str(settings)).replace(TEST_FOLDER, str(test_folder))
    copy.write_text(code, encoding="utf-8")
    return copy


@dataclass(frozen=True)
class Outcome:
    """How one example ended.

    Attributes:
        name: The example's file name.
        status: `PASSED`, `REFUSED`, `NOT_LISTED`, or `FAILED`.
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


def guide_blocks(text: str) -> dict[int, str]:
    """Read the guide's example blocks: each numbered example heading, and its code.

    Args:
        text: The guide, `docs/tester-guide.md`.

    Returns:
        Each example's number and the code of the first Python block under its heading,
        before the next heading, ending with a newline, as the file does.
    """
    blocks: dict[int, str] = {}
    number = 0  # the example whose heading came last; 0 outside an example
    code: list[str] | None = None
    for line in text.splitlines():
        if code is not None:
            if line.rstrip() == "```":
                blocks[number] = "\n".join(code) + "\n"
                number, code = 0, None
            else:
                code.append(line)
        elif line.startswith("#"):
            heading = GUIDE_HEADING.match(line)
            number = int(heading.group(1)) if heading else 0
        elif number and line.rstrip() == "```python":
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


def failed_streams(session: dm.MassiveSession) -> list[str]:
    """Return the `Error` of each of a session's streams that failed.

    Args:
        session: The session.

    Returns:
        The `Error` of each row of `streams()` whose `State` is `FAILED`; none for a closed
        session, whose streams were stopped, not failed.
    """
    try:
        frame = to_pandas(session.streams().snapshot())
    except dm.MassiveError:  # a closed session's streams can no longer be read
        return []
    return [str(error) for error in frame.loc[frame["State"] == "FAILED", "Error"]]


def stream_verdict(outcome: Outcome, errors: list[str]) -> Outcome:
    """Give a passed example the verdict of the streams and reads that failed after it returned.

    Args:
        outcome: The example's outcome when it returned.
        errors: The `Error` of each failed row of `m.streams()` of the sessions it opened
            (`failed_streams`): streams, and reads in the background.

    Returns:
        The outcome as it was when it isn't `PASSED` or nothing failed. Otherwise, when every
        failed row was refused, by Massive (`STREAM_REFUSAL`, `READ_REFUSAL`) or by the
        settings (`DELAYED_REFUSAL`): `NOT_LISTED` when the settings refused any, the one the
        tester can fix, and `REFUSED` when Massive refused them all; and `FAILED` when any row
        failed for another reason. The new outcome's error names the first failed row's
        `Error`.
    """
    if outcome.status != PASSED or not errors:
        return outcome
    refusals = (STREAM_REFUSAL, READ_REFUSAL, DELAYED_REFUSAL)
    if not all(any(words in error for words in refusals) for error in errors):
        status = FAILED
    elif any(DELAYED_REFUSAL in error for error in errors):
        status = NOT_LISTED
    else:
        status = REFUSED
    return Outcome(outcome.name, status, outcome.seconds, f"a stream failed: {errors[0]}")


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
    except Exception as failure:  # noqa: BLE001 - any other error of an example is a finding
        status, error = _failure_outcome(failure)
    return Outcome(path.name, status, round(clock() - start, 1), error), printed.getvalue()


def _failure_outcome(failure: Exception) -> tuple[str, str]:
    """Judge what an example raised.

    Args:
        failure: The error, being handled.

    Returns:
        The status and the error's text: `NOT_LISTED` for the settings' `EntitlementError`
        (its `source` is `ErrorSource.USER`), `REFUSED` for Massive's
        (`refusals.is_massive_refusal`), each with its type and message; otherwise `FAILED`,
        with its first line and its traceback, the library's own refusals included.
    """
    if isinstance(failure, dm.EntitlementError) and failure.source is dm.ErrorSource.USER:
        return NOT_LISTED, f"{type(failure).__name__}: {failure}"
    if is_massive_refusal(failure):
        return REFUSED, f"{type(failure).__name__}: {failure}"
    headline = f"{type(failure).__name__}: {str(failure).strip().partition(chr(10))[0]}"
    return FAILED, f"{headline}\n{traceback.format_exc()}"


def tables_of(namespace: dict[str, Any]) -> dict[str, Table]:
    """Return a namespace's table variables.

    Args:
        namespace: The examples' shared variables.

    Returns:
        Each variable that holds a Deephaven table, by name; private names left out.
    """
    return {n: v for n, v in namespace.items() if isinstance(v, Table) and not n.startswith("_")}


def save_tables(tables: dict[str, Table], folder: Path, secrets: SecretFilter) -> list[str]:
    """Save the first rows of each table as a CSV file, cleaned of the secrets.

    A table that cannot be read, such as a refreshing table that failed after its example
    returned, gets `<variable>.error.txt` with the error's first line instead, and the run
    goes on.

    Args:
        tables: The tables, by variable name.
        folder: Where the files go, created if missing.
        secrets: The secrets to keep out of the files.

    Returns:
        One line per table that could not be saved: its name and the error's first line.
    """
    unsaved: list[str] = []
    for name, table in tables.items():
        folder.mkdir(parents=True, exist_ok=True)
        try:
            text = to_pandas(table.head(TABLE_ROWS).snapshot()).to_csv(index=False)
        except Exception as failure:  # noqa: BLE001 - a failed table is recorded, not fatal
            line = f"{name}: {type(failure).__name__}: {str(failure).strip().partition(chr(10))[0]}"
            (folder / f"{name}.error.txt").write_text(secrets.clean(line) + "\n", encoding="utf-8")
            unsaved.append(line)
        else:
            (folder / f"{name}.csv").write_text(secrets.clean(text), encoding="utf-8")
    return unsaved


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
    test_folder: Path,
    *,
    secrets: SecretFilter,
    echo: Callable[[str], None],
    lock: Callable[[], AbstractContextManager[None]] = console_lock,
    wait: Callable[[dict[str, Any]], bool] = wait_for_downloads,
    delayed: Collection[str] = (),
    settle: Callable[[list[dm.MassiveSession]], None] = lambda _sessions: time.sleep(REFUSAL_WAIT),
) -> list[Outcome]:
    """Run the examples in order, as one console session, and save what each did.

    Args:
        files: The example files, in order (`example_files`).
        out: The output folder, created if missing.
        test_folder: The tester's folder, where the examples write their debug log and the
            diagnostics in place of `TEST_FOLDER` (`fitted`).
        secrets: The secrets to keep out of every file written and line echoed.
        echo: Prints one line of progress.
        lock: Makes the lock every console command holds.
        wait: Waits for the background downloads before each example; returns False when it
            gave up.
        delayed: The subscriptions the settings file `out/settings.yaml` lists as delayed
            (`settings_text`); the default lists none. An example
            that names `SETTINGS_PATH` or `TEST_FOLDER` runs from a copy under `out/fitted/`
            (`fitted`).
        settle: Waits, after the last example, for Massive's answers to the streams of the
            sessions the examples opened, which it is given; called only when an example
            opened a session.

    Returns:
        Each example's outcome, with the verdict of the streams its sessions failed
        (`stream_verdict`).
    """
    out.mkdir(parents=True, exist_ok=True)
    settings = out / "settings.yaml"
    settings.write_text(settings_text(delayed), encoding="utf-8")
    namespace: dict[str, Any] = {}
    outcomes: list[Outcome] = []
    opened: dict[int, tuple[dm.MassiveSession, str]] = {}
    with console_scope(namespace):
        for path in files:
            if not wait(namespace):
                echo("a background download is still running; going on without it")
            before = dict(tables_of(namespace))
            echo(f"{path.name}: running")
            runs = fitted(path, settings=settings, test_folder=test_folder, folder=out / "fitted")
            outcome, printed = run_example(runs, namespace, lock=lock)
            outcomes.append(outcome)
            for session in sessions(namespace):
                opened.setdefault(id(session), (session, path.name))
            made = {n: t for n, t in tables_of(namespace).items() if before.get(n) is not t}
            unsaved = save_tables(made, out / "tables" / path.stem, secrets)
            text = printed + (f"\n{outcome.error}" if outcome.error else "")
            text += "".join(f"\ntable not saved: {line}" for line in unsaved)
            (out / f"{path.stem}.txt").write_text(secrets.clean(text), encoding="utf-8")
            echo(secrets.clean(summary_line(outcome)))
            for line in unsaved:
                echo(secrets.clean(f"  table not saved: {line}"))
        if opened:
            settle([session for session, _ in opened.values()])
        errors: dict[str, list[str]] = {}
        for session, name in opened.values():
            errors.setdefault(name, []).extend(failed_streams(session))
        for index, outcome in enumerate(outcomes):
            verdict = stream_verdict(outcome, errors.get(outcome.name, []))
            if verdict is not outcome:
                outcomes[index] = verdict
                echo(secrets.clean(summary_line(verdict)))
                stem = verdict.name.removesuffix(".py")
                with (out / f"{stem}.txt").open("a", encoding="utf-8") as file:
                    file.write(secrets.clean(f"\n{verdict.error}"))
        # Every session an example opened is closed on the way out, the replaced ones too.
        namespace.update({f"_opened_{i}": s for i, (s, _) in enumerate(opened.values())})
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
