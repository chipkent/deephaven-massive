r"""Decide how much of the local full check a branch's changes need, by CI's own path rules.

`scripts/check.sh full`, which the pre-push hook runs, asks this script which scope to run. CI
already skips its test jobs when a pull request changes only files the tests cannot read, such
as the developer guides; the local check makes the same choice, so a push of such
a change no longer waits minutes for the whole suite. The local check never runs fewer tests
than CI's test jobs run for the same change, so a push the hook passes cannot fail one of those
jobs on a test the hook skipped. CI also builds the package and tests other Python versions and
the lowest dependency versions, which the local check does not.

The path rules are not copied here. The script reads them from the `Decide which jobs the
change affects` step of `.github/workflows/ci.yml`, the shell variables `test`, `docs`,
`unread`, and `workflows` (`lint` and `build` too, which a path must match or be unread), and
applies them exactly as that step does, one changed path at a time:

- A changed workflow or composite action runs everything.
- A path that no rule covers runs everything, so a new kind of input fails closed.
- A path the `test` pattern matches runs the whole suite.
- A path the `docs` pattern matches (the user guide, `README.md` and `docs/guide/`, its
  images, and the tester guide) runs the documentation tests, `tests/test_docs.py` and
  `tests/live/tester/test_examples.py`, as CI's `docs` job does.
- Otherwise CI runs no tests, and the local check runs ruff, format, and mypy. CI runs ruff
  and mypy on every change it lints, and the local check runs them on every change, so it
  never does less.

The changed files are those `git diff --no-renames --name-only` lists between the merge base of
`HEAD` with `origin/main` and the working tree (so staged and unstaged edits count), plus
untracked files git does not ignore. Like CI's list, a path git quotes (one holding a tab, a
newline, or a non-ASCII character) matches no rule and runs everything. Without `origin/main`
or a merge base, the script runs everything.

It prints the scope, one of `full`, `docs`, or `lint`, on standard output, and one line on
standard error that says which scope it chose and why. It uses only the standard library.

    uv run python scripts/check_scope.py

The examples below are doctests, which `check.sh full` runs before it asks for a scope:

>>> rules = PathRules(
...     lint=r"^(src|tests|scripts)/|^pyproject\.toml$",
...     test=r"^(src|tests|scripts|vendor)/|^docs/datasets/|^pyproject\.toml$",
...     build=r"^(src|tests)/|^docs/datasets/|^README\.md$",
...     docs=r"^README\.md$|^docs/(guide|assets)/|^docs/tester-guide\.md$",
...     unread=r"^(docs/dev/|AGENTS\.md$|\.pre-commit-config\.yaml$)",
...     workflows=r"^\.github/(workflows|actions)/",
... )
>>> decide(["docs/dev/pull-requests.md", "AGENTS.md"], rules)
Decision(scope='lint', reason='2 changed files, none of which the tests read')
>>> decide(["AGENTS.md", "README.md"], rules)
Decision(scope='docs', reason='2 changed files; README.md is documentation')
>>> decide(["docs/dev/x.md", "docs/guide/history.md"], rules)
Decision(scope='docs', reason='2 changed files; docs/guide/history.md is documentation')
>>> decide(["AGENTS.md", "src/deephaven_massive/__init__.py", "tests/conftest.py"], rules)
Decision(scope='full', reason='3 changed files; src/deephaven_massive/__init__.py is a test input')
>>> decide(["docs/dev/x.md", "docs/datasets/stocks.trades.md"], rules)
Decision(scope='full', reason='2 changed files; docs/datasets/stocks.trades.md is a test input')
>>> decide(["docs/dev/x.md", ".github/workflows/ci.yml"], rules)
Decision(scope='full', reason='2 changed files; .github/workflows/ci.yml is a workflow')
>>> decide(["docs/dev/x.md", "docs/tester-examples/x.py"], rules)
Decision(scope='full', reason='2 changed files; no rule covers docs/tester-examples/x.py')
>>> decide(['"docs/dev/caf\\303\\251.md"'], rules)
Decision(scope='full', reason='1 changed file; no rule covers "docs/dev/caf\\303\\251.md"')
>>> decide([], rules)
Decision(scope='lint', reason='no changed files')
>>> read_rules("lint='x'\n")
Traceback (most recent call last):
...
ValueError: .github/workflows/ci.yml defines no test pattern for the changed-files decision
"""

from __future__ import annotations

import pathlib
import re
import shutil
import subprocess
import sys
from typing import NamedTuple

#: The workflow whose path rules the decision applies.
WORKFLOW = pathlib.Path(".github/workflows/ci.yml")

#: The branch CI compares a pull request with.
BASE = "origin/main"

_ASSIGNMENT = re.compile(r"^\s*(lint|test|build|docs|unread|workflows)='(.*)'\s*$", re.MULTILINE)


class PathRules(NamedTuple):
    """The patterns of CI's changed-files decision, each an extended regular expression."""

    lint: str
    test: str
    build: str
    docs: str
    unread: str
    workflows: str


class Decision(NamedTuple):
    """The scope to run, and the reason the one-line summary gives for it."""

    scope: str
    reason: str


def read_rules(workflow_text: str) -> PathRules:
    """Return the path patterns that CI's decision step assigns, read from the workflow text.

    Args:
        workflow_text: The contents of `.github/workflows/ci.yml`.

    Returns:
        The six patterns, exactly as the step assigns them.

    Raises:
        ValueError: The text assigns one of the patterns not at all, or more than once, so the
            rules cannot be read with certainty.
    """
    found: dict[str, list[str]] = {}
    for match in _ASSIGNMENT.finditer(workflow_text):
        found.setdefault(match[1], []).append(match[2])
    patterns: dict[str, str] = {}
    for name in PathRules._fields:
        values = found.get(name, [])
        if len(values) != 1:
            problem = "no" if not values else "more than one"
            message = f"{WORKFLOW} defines {problem} {name} pattern for the changed-files decision"
            raise ValueError(message)
        patterns[name] = values[0]
    return PathRules(**patterns)


def _plural(count: int) -> str:
    """Return `"<count> changed file"` with the right plural."""
    return f"{count} changed file{'' if count == 1 else 's'}"


def decide(paths: list[str], rules: PathRules) -> Decision:
    """Return the scope a change needs, by the rules CI's decision step applies.

    Args:
        paths: The changed paths, relative to the repository root, as git lists them.
        rules: CI's path patterns, from `read_rules`.

    Returns:
        `full` when a path is a workflow, matches no rule, or is a test input; else `docs`
        when a path is documentation the tests read; else `lint`. The reason names the count
        of changed files and the first path, in the given order, that forced the full run.
    """
    if not paths:
        return Decision("lint", "no changed files")
    count = _plural(len(paths))
    covered = f"{rules.lint}|{rules.test}|{rules.build}|{rules.docs}|{rules.unread}"
    checks = (
        (rules.workflows, True, "is a workflow"),
        (covered, False, None),
        (rules.test, True, "is a test input"),
    )
    for pattern, forces_when_matched, what in checks:
        for path in paths:
            if bool(re.search(pattern, path)) == forces_when_matched:
                reason = f"{path} {what}" if what else f"no rule covers {path}"
                return Decision("full", f"{count}; {reason}")
    for path in paths:
        if re.search(rules.docs, path):
            return Decision("docs", f"{count}; {path} is documentation")
    return Decision("lint", f"{count}, none of which the tests read")


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


def changed_paths() -> list[str] | str:
    """Return the paths this branch and its working tree change against `origin/main`.

    Returns:
        The changed and untracked paths, in git's order, without repeats; or, when they cannot
        be listed, the reason, for the summary line.
    """
    base = _git("merge-base", BASE, "HEAD")
    if base is None:
        return f"no merge base with {BASE}; run git fetch"
    diff = _git("diff", "--no-renames", "--name-only", base.strip())
    untracked = _git("ls-files", "--others", "--exclude-standard")
    if diff is None or untracked is None:
        return "git could not list the changed files"
    return list(dict.fromkeys([*diff.splitlines(), *untracked.splitlines()]))


def main() -> int:
    """Print the scope on standard output and the summary line on standard error.

    Returns:
        0 always: a failure to list the changes selects the full scope instead.
    """
    rules = read_rules(WORKFLOW.read_text(encoding="utf-8"))
    paths = changed_paths()
    decision = Decision("full", paths) if isinstance(paths, str) else decide(paths, rules)
    sys.stdout.write(f"{decision.scope}\n")
    sys.stderr.write(f"check.sh: scope {decision.scope}: {decision.reason}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
