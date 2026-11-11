r"""Decide how much of the local full check a change needs, by CI's own path rules.

`scripts/check.sh full`, which the pre-push hook runs, asks this script which scope to run. CI
already skips its test jobs when a pull request changes only files the tests cannot read, such
as the developer guides; the local check makes the same choice, so a push of such
a change no longer waits minutes for the whole suite. The local check never runs fewer tests
than CI's test jobs run for the same change, so a push the hook passes cannot fail one of those
jobs on a test the hook skipped. CI also builds the package and tests other Python versions and
the lowest dependency versions, which the local check does not.

The path rules are not copied here. The script reads them from the `Decide which jobs the
change affects` step of `.github/workflows/ci.yml`, the shell variables `test`, `docs`, `site`,
`unread`, and `workflows` (`lint` and `build` too, which a path must match or be unread), and
applies them exactly as that step does, one changed path at a time:

- A changed workflow or composite action runs everything.
- A path that no rule covers runs everything, so a new kind of input fails closed.
- A path the `test` pattern matches runs the whole suite.
- A path the `docs` pattern matches (the user pages `README.md` and `docs/troubleshooting.md`,
  any image under `docs/`, `CONTRIBUTING.md`, the API reference pages under `docs/api/` and
  `zensical.toml`, which lists them, the tester guide, and its examples) runs the documentation
  tests, `tests/test_docs.py` and `tests/live/tester/test_examples.py`, as CI's `docs` job does.
  `docs/configuration.md` is a test input instead: `tests/test_config.py` compares its
  generated settings reference with the code.
- Otherwise CI runs no tests, and the local check runs ruff, format, and mypy. CI runs ruff
  and mypy on every change it lints, and the local check runs them on every change, so it
  never does less.

Separately, a path the `site` pattern matches (the documentation site's configuration, the
pages it publishes, and the package source its API reference reads), a changed workflow, or a
path no rule covers builds the documentation site in strict mode, as CI's `site` job does.

The changed files are those `git diff --no-renames --name-only` lists between a base commit and
the working tree (so staged and unstaged edits count), plus untracked files git does not ignore.
In a git hook, pre-commit hides unstaged edits to tracked files first, so there only staged
edits and untracked files count, and the tests do not see the hidden edits either.
The base depends on how the script runs:

- In the pre-push hook, pushing to a branch the remote already has, the base is that branch's
  remote tip. pre-commit names it in `PRE_COMMIT_FROM_REF` and the remote branch in
  `PRE_COMMIT_REMOTE_NAME` and `PRE_COMMIT_REMOTE_BRANCH`; the script uses it when it equals
  the local remote-tracking branch. So a push that changes only a developer guide on a
  long-lived PR branch runs no tests, even when earlier pushes changed code: CI already ran
  the tests on those. A tree diff is used, not a merge base, so the files a merge of `main`
  brings in count as changed.
- Otherwise (a new branch, a stale or missing remote-tracking branch, or a run by hand), the
  base is the merge base of `HEAD` with `origin/main`: the whole branch, as CI compares it.

Like CI's list, a path git quotes (one holding a tab, a newline, or a non-ASCII character)
matches no rule and runs everything. Without `origin/main` or a merge base, the script runs
everything.

It prints the scope, one of `full`, `docs`, or `lint`, and then `site` or `no-site`, on one line
of standard output, and one line on standard error that says which scope it chose and why. It
uses only the standard library.

    uv run python scripts/check_scope.py

The examples below are doctests, which `check.sh full` runs before it asks for a scope:

>>> rules = PathRules(
...     lint=r"^(src|tests|scripts)/|^pyproject\.toml$",
...     test=r"^(src|tests|scripts|vendor)/|^docs/datasets/|^pyproject\.toml$",
...     build=r"^(src|tests)/|^docs/datasets/|^README\.md$",
...     docs=r"^(README|CONTRIBUTING)\.md$|^docs/troubleshooting\.md$|^docs/.+\.(png|webp)$",
...     site=r"^zensical\.toml$|^README\.md$|^docs/troubleshooting\.md$|^src/",
...     unread=r"^(docs/dev/|AGENTS\.md$|\.pre-commit-config\.yaml$)",
...     workflows=r"^\.github/(workflows|actions)/",
... )
>>> decide(["docs/dev/pull-requests.md", "AGENTS.md"], rules)
Decision(scope='lint', reason='2 changed files, none of which the tests read')
>>> decide(["AGENTS.md", "README.md"], rules)
Decision(scope='docs', reason='2 changed files; README.md is documentation')
>>> decide(["docs/dev/x.md", "docs/troubleshooting.md"], rules)
Decision(scope='docs', reason='2 changed files; docs/troubleshooting.md is documentation')
>>> decide(["CONTRIBUTING.md"], rules)
Decision(scope='docs', reason='1 changed file; CONTRIBUTING.md is documentation')
>>> decide(["docs/images/hero.webp"], rules)
Decision(scope='docs', reason='1 changed file; docs/images/hero.webp is documentation')
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
>>> builds_site(["AGENTS.md", "README.md"], rules)
True
>>> builds_site(["docs/dev/x.md", "tests/conftest.py"], rules)
False
>>> builds_site(["zensical.toml"], rules)
True
>>> builds_site(["docs/dev/x.md", ".github/workflows/ci.yml"], rules)
True
>>> builds_site(["docs/tester-examples/x.py"], rules)
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
>>> read_rules("lint='x'\n")
Traceback (most recent call last):
...
ValueError: .github/workflows/ci.yml defines no test pattern for the changed-files decision
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
    from collections.abc import Mapping

#: The workflow whose path rules the decision applies.
WORKFLOW = pathlib.Path(".github/workflows/ci.yml")

#: The branch CI compares a pull request with.
BASE = "origin/main"

_ASSIGNMENT = re.compile(
    r"^\s*(lint|test|build|docs|site|unread|workflows)='(.*)'\s*$", re.MULTILINE
)


class PathRules(NamedTuple):
    """The patterns of CI's changed-files decision, each an extended regular expression."""

    lint: str
    test: str
    build: str
    docs: str
    site: str
    unread: str
    workflows: str


class Push(NamedTuple):
    """The remote tip a pre-push hook names, and the local ref that should hold the same commit."""

    from_ref: str
    tracking_ref: str


class Decision(NamedTuple):
    """The scope to run, and the reason the one-line summary gives for it."""

    scope: str
    reason: str


def read_rules(workflow_text: str) -> PathRules:
    """Return the path patterns that CI's decision step assigns, read from the workflow text.

    Args:
        workflow_text: The contents of `.github/workflows/ci.yml`.

    Returns:
        The seven patterns, exactly as the step assigns them.

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


def _covered(rules: PathRules) -> str:
    """Return the pattern of every path some rule covers, as CI's decision step joins them."""
    return f"{rules.lint}|{rules.test}|{rules.build}|{rules.docs}|{rules.site}|{rules.unread}"


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
    covered = _covered(rules)
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


def builds_site(paths: list[str], rules: PathRules) -> bool:
    """Return whether a change needs the documentation site built, by CI's rules for its job.

    Args:
        paths: The changed paths, relative to the repository root, as git lists them.
        rules: CI's path patterns, from `read_rules`.

    Returns:
        True when a path is a workflow, matches no rule, or is one of the site's sources.
    """
    covered = _covered(rules)
    return any(
        re.search(rules.workflows, path)
        or not re.search(covered, path)
        or re.search(rules.site, path)
        for path in paths
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
    """Print the scope and the site decision on standard output, and a summary on standard error.

    Returns:
        0 always: a failure to list the changes selects the full scope and the site build
        instead.
    """
    rules = read_rules(WORKFLOW.read_text(encoding="utf-8"))
    found = changed_paths()
    if isinstance(found, str):
        decision, against, site = Decision("full", found), "", True
    else:
        paths, base = found
        decision, against = decide(paths, rules), f" ({base})"
        site = builds_site(paths, rules)
    site_word = "site" if site else "no-site"
    sys.stdout.write(f"{decision.scope} {site_word}\n")
    sys.stderr.write(
        f"check.sh: scope {decision.scope}{against}: {decision.reason}; "
        f"{'builds' if site else 'does not build'} the documentation site\n"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
