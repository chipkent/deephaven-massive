r"""Choose the tests the pre-push hook runs: those of the files a change touches.

`scripts/check.sh full`, which the pre-push hook runs, asks this script which tests to run. The
hook runs ruff and mypy on every push, and then only the tests of the changed files, without
coverage; CI runs the whole suite with coverage on every pull request. So a push waits for the
few tests its change is most likely to break, and CI catches the rest.

The tests of a change are, in order and without repeats:

- each changed test file (`tests/**/test_*.py`) that still exists;
- the unit test that mirrors each changed module of the library (`src/deephaven_massive/`), of
  `scripts/`, or of a helper package under `tests/`, when it exists. The mirror drops one
  leading underscore from each path component and maps a dunder module to its bare name, as
  `tests/test_layout.py` requires: `src/deephaven_massive/_massive/rest/client.py` has
  `tests/massive/rest/test_client.py`, and `tests/_live/pacing.py` has
  `tests/live/test_pacing.py`;
- the documentation tests, `tests/test_docs.py` and `tests/live/tester/test_examples.py`, when
  a user page, an image under `docs/`, the tester guide, or its examples changed: the paths
  the `docs` pattern of CI's `Decide which jobs the change affects` step matches, read from
  `.github/workflows/ci.yml`.

Any other change (a conftest, a fake server, test data, generated files, the configuration)
selects no test of its own; CI's whole suite covers it.

The changed files are those `git diff --no-renames --name-only` lists between a base commit and
the working tree (so staged and unstaged edits count), plus untracked files git does not ignore.
In a git hook, pre-commit hides unstaged edits to tracked files first, so there only staged
edits and untracked files count, and the tests do not see the hidden edits either.
The base depends on how the script runs:

- In the pre-push hook, pushing to a branch the remote already has, the base is that branch's
  remote tip. pre-commit names it in `PRE_COMMIT_FROM_REF` and the remote branch in
  `PRE_COMMIT_REMOTE_NAME` and `PRE_COMMIT_REMOTE_BRANCH`; the script uses it when it equals
  the local remote-tracking branch. So a push runs the tests of its own commits only: CI
  already ran the tests of earlier pushes. A tree diff is used, not a merge base, so the files
  a merge of `main` brings in count as changed.
- Otherwise (a new branch, a stale or missing remote-tracking branch, or a run by hand), the
  base is the merge base of `HEAD` with `origin/main`: the whole branch, as CI compares it.

Without `origin/main` or a merge base, the script selects no tests and says why.

It prints one line on standard output, the scope and then the test files: `lint` when no test
is selected, or `tests` followed by each file, separated by spaces (no path in the repository
holds a space). It prints one line on standard error that says what it chose and why. It uses
only the standard library.

    uv run python scripts/check_scope.py

The examples below are doctests, which `check.sh full` runs before it asks for a scope:

>>> docs = r"^README\.md$|^docs/troubleshooting\.md$|^docs/.+\.(png|webp)$"
>>> exists = {
...     "tests/massive/rest/test_client.py",
...     "tests/test_init.py",
...     "tests/live/test_pacing.py",
...     "tests/test_layout.py",
...     *DOCS_TESTS,
... }.__contains__
>>> decide(["docs/dev/pull-requests.md", "AGENTS.md"], docs, exists)
Decision(scope='lint', tests=(), reason='2 changed files, none with a test')
>>> decision = decide(["src/deephaven_massive/_massive/rest/client.py"], docs, exists)
>>> decision.scope, decision.tests, decision.reason
('tests', ('tests/massive/rest/test_client.py',), '1 changed file; 1 test file')
>>> decide(["src/deephaven_massive/__init__.py", "tests/test_init.py"], docs, exists).tests
('tests/test_init.py',)
>>> decide(
...     ["tests/_live/pacing.py", "tests/test_layout.py", "tests/conftest.py"], docs, exists
... ).tests
('tests/live/test_pacing.py', 'tests/test_layout.py')
>>> decide(["src/deephaven_massive/_session.py", "tests/test_gone.py"], docs, exists)
Decision(scope='lint', tests=(), reason='2 changed files, none with a test')
>>> decide(["AGENTS.md", "README.md"], docs, exists).tests
('tests/test_docs.py', 'tests/live/tester/test_examples.py')
>>> decide([], docs, exists)
Decision(scope='lint', tests=(), reason='no changed files')
>>> mirror_of("src/deephaven_massive/_tables/__init__.py")
'tests/tables/test_init.py'
>>> mirror_of("scripts/check_scope.py")
'tests/scripts/test_check_scope.py'
>>> mirror_of("tests/_fakes/rest.py")
'tests/fakes/test_rest.py'
>>> mirror_of("docs/dev/x.md") is None
True
>>> pushed_branch(
...     {
...         "PRE_COMMIT_FROM_REF": "1f0c2e9",
...         "PRE_COMMIT_REMOTE_NAME": "origin",
...         "PRE_COMMIT_REMOTE_BRANCH": "refs/heads/docs/new-image",
...     }
... )
Push(from_ref='1f0c2e9', tracking_ref='refs/remotes/origin/docs/new-image')
>>> pushed_branch({}) is None
True
>>> pushed_branch(
...     {
...         "PRE_COMMIT_FROM_REF": "1f0c2e9",
...         "PRE_COMMIT_REMOTE_NAME": "origin",
...         "PRE_COMMIT_REMOTE_BRANCH": "refs/tags/v1.0.0",
...     }
... ) is None
True
>>> read_docs_pattern("lint='x'\n")
Traceback (most recent call last):
...
ValueError: .github/workflows/ci.yml defines no docs pattern for the changed-files decision
"""

from __future__ import annotations

import os
import pathlib
import re
import shutil
import subprocess
import sys
from typing import TYPE_CHECKING, NamedTuple

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

#: The workflow whose `docs` pattern selects the documentation tests.
WORKFLOW = pathlib.Path(".github/workflows/ci.yml")

#: The branch CI compares a pull request with.
BASE = "origin/main"

#: The tests CI's `docs` job runs when a user page changes.
DOCS_TESTS = ("tests/test_docs.py", "tests/live/tester/test_examples.py")

#: The directories whose Python modules have a mirrored unit test, each with the prefix its
#: mirror goes under. A module under `tests/` mirrors only from a helper package, whose name
#: starts with an underscore.
_MIRRORED = (
    ("src/deephaven_massive/", "tests/"),
    ("scripts/", "tests/scripts/"),
    ("tests/_", "tests/"),
)

_DOCS_ASSIGNMENT = re.compile(r"^\s*docs='(.*)'\s*$", re.MULTILINE)


class Push(NamedTuple):
    """The remote tip a pre-push hook names, and the local ref that should hold the same commit."""

    from_ref: str
    tracking_ref: str


class Decision(NamedTuple):
    """The scope to run, the test files it runs, and the reason the summary line gives."""

    scope: str
    tests: tuple[str, ...]
    reason: str


def read_docs_pattern(workflow_text: str) -> str:
    """Return the `docs` path pattern that CI's decision step assigns, read from the workflow.

    Args:
        workflow_text: The contents of `.github/workflows/ci.yml`.

    Returns:
        The pattern, an extended regular expression, exactly as the step assigns it.

    Raises:
        ValueError: The text assigns the pattern not at all, or more than once, so it cannot be
            read with certainty.
    """
    found: list[str] = [match[1] for match in _DOCS_ASSIGNMENT.finditer(workflow_text)]
    if len(found) != 1:
        problem = "no" if not found else "more than one"
        raise ValueError(
            f"{WORKFLOW} defines {problem} docs pattern for the changed-files decision"
        )
    return found[0]


def _mirror_name(name: str) -> str:
    """Return a path component's mirrored name: a dunder's bare name, else one underscore less."""
    # `__` alone, or `____`, has no name between its underscores, so it is not a dunder.
    bare = name[2:-2]
    if bare and name.startswith("__") and name.endswith("__"):
        return bare
    return name.removeprefix("_")


def mirror_of(path: str) -> str | None:
    """Return the unit test that mirrors a Python module, whether or not that test exists.

    Args:
        path: A changed path, relative to the repository root.

    Returns:
        The mirrored test file's path; or `None` for a path that is not a module of the
        library, of `scripts/`, or of a helper package under `tests/`.
    """
    if not path.endswith(".py"):
        return None
    for prefix, mirror in _MIRRORED:
        if path.startswith(prefix):
            module = pathlib.PurePosixPath(path.removeprefix(prefix))
            directories = [_mirror_name(part) for part in module.parent.parts]
            test = pathlib.PurePosixPath(*directories, f"test_{_mirror_name(module.stem)}.py")
            return f"{mirror}{test}"
    return None


def _tests_of(path: str, docs: str) -> tuple[str, ...]:
    """Return the test files one changed path selects, before any existence check."""
    name = pathlib.PurePosixPath(path).name
    if path.startswith("tests/") and name.startswith("test_") and name.endswith(".py"):
        return (path,)
    mirror = mirror_of(path)
    if mirror is not None:
        return (mirror,)
    if re.search(docs, path):
        return DOCS_TESTS
    return ()


def _plural(count: int) -> str:
    """Return `"<count> changed file"` with the right plural."""
    return f"{count} changed file{'' if count == 1 else 's'}"


def decide(paths: list[str], docs: str, exists: Callable[[str], bool]) -> Decision:
    """Return the tests a change selects: its changed tests, its mirrors, and the docs tests.

    Args:
        paths: The changed paths, relative to the repository root, as git lists them.
        docs: CI's `docs` pattern, from `read_docs_pattern`.
        exists: Whether a test file exists, given its path relative to the repository root.

    Returns:
        `tests` with the selected files that exist, in the order of the paths that selected
        them and without repeats; or `lint` with none. The reason counts the changed files and
        the selected test files.
    """
    if not paths:
        return Decision("lint", (), "no changed files")
    selected = [test for path in paths for test in _tests_of(path, docs)]
    tests = tuple(test for test in dict.fromkeys(selected) if exists(test))
    count = _plural(len(paths))
    if not tests:
        return Decision("lint", (), f"{count}, none with a test")
    return Decision(
        "tests", tests, f"{count}; {len(tests)} test file{'' if len(tests) == 1 else 's'}"
    )


def _git(*args: str) -> str | None:
    """Return a git command's standard output, or `None` when git is missing or fails."""
    git = shutil.which("git")
    if git is None:
        return None
    result = subprocess.run(  # noqa: S603 - git with fixed arguments, no shell
        [git, *args],
        capture_output=True,
        text=True,
        check=False,
    )
    return result.stdout if result.returncode == 0 else None


def pushed_branch(environ: Mapping[str, str]) -> Push | None:
    """Return the branch a pre-commit pre-push hook pushes to, from the variables it sets.

    Args:
        environ: The process environment.

    Returns:
        The commit pre-commit compares the push with (`PRE_COMMIT_FROM_REF`) and the
        remote-tracking ref of the pushed branch; or `None` outside a pre-push hook, or when the
        push goes to a ref that is not a branch, such as a tag.
    """
    from_ref = environ.get("PRE_COMMIT_FROM_REF")
    remote = environ.get("PRE_COMMIT_REMOTE_NAME")
    branch = environ.get("PRE_COMMIT_REMOTE_BRANCH", "")
    if not from_ref or not remote or not branch.startswith("refs/heads/"):
        return None
    return Push(from_ref, f"refs/remotes/{remote}/{branch.removeprefix('refs/heads/')}")


def comparison_base() -> tuple[str, str] | str:
    """Return the commit the changed paths are listed against, and a description of it.

    For a push to a branch the remote already has, that is the branch's remote tip. For a new
    branch, pre-commit names the parent of the first commit no remote has, which no
    remote-tracking ref of the pushed branch matches, so the base is the merge base with
    `origin/main`, as it is outside the hook.

    Returns:
        The base commit and the words the summary line uses for it; or, when there is no base,
        the reason, for the summary line.
    """
    push = pushed_branch(os.environ)
    if push is not None:
        tip = _git("rev-parse", "--verify", "--quiet", f"{push.tracking_ref}^{{commit}}")
        from_commit = _git("rev-parse", "--verify", "--quiet", f"{push.from_ref}^{{commit}}")
        if tip is not None and tip == from_commit:
            name = push.tracking_ref.removeprefix("refs/remotes/")
            return tip.strip(), f"since {name}"
    base = _git("merge-base", BASE, "HEAD")
    if base is None:
        return f"no merge base with {BASE}; run git fetch"
    return base.strip(), f"against {BASE}"


def changed_paths() -> tuple[list[str], str] | str:
    """Return the paths the working tree changes against the comparison base.

    Returns:
        The changed and untracked paths, in git's order, without repeats, and the words that
        describe the base; or, when they cannot be listed, the reason, for the summary line.
    """
    found = comparison_base()
    if isinstance(found, str):
        return found
    base, against = found
    diff = _git("diff", "--no-renames", "--name-only", base)
    untracked = _git("ls-files", "--others", "--exclude-standard")
    if diff is None or untracked is None:
        return "git could not list the changed files"
    return list(dict.fromkeys([*diff.splitlines(), *untracked.splitlines()])), against


def main() -> int:
    """Print the scope and its tests on standard output, and the summary line on standard error.

    Returns:
        0 always: a failure to list the changes selects no tests, and says so.
    """
    docs = read_docs_pattern(WORKFLOW.read_text(encoding="utf-8"))
    found = changed_paths()
    if isinstance(found, str):
        decision, against = Decision("lint", (), found), ""
    else:
        paths, base = found
        decision, against = (
            decide(paths, docs, lambda test: pathlib.Path(test).is_file()),
            f" ({base})",
        )
    sys.stdout.write(" ".join((decision.scope, *decision.tests)) + "\n")
    sys.stderr.write(f"check.sh: scope {decision.scope}{against}: {decision.reason}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
