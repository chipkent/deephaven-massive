"""Check a pull request description for the approval rules a program can check.

The owner merges from the description and asks to be asked directly: a question that waits in
a description means the work was not ready for a pull request or for CI. Three failures came
through descriptions in one week: a change listed as a choice "for the owner to confirm" and
merged unconfirmed; an approval claimed in the author's own words ("owner-approved", "the
owner decided on 2026-11-09") that no owner message contains; and a rule in `AGENTS.md`
rewritten with no record of the decisions it replaced. This script fails on each shape
(`AGENTS.md`, an approval is a quote; the `pre-push-review` skill, section 4):

- **An item that asks the owner anything**, from the `**Check before merging` heading to the
  next `## ` heading: a `?` outside quotes, code, and links, or "open:", "to confirm",
  "unconfirmed", "for the owner/you to confirm/decide/judge/settle", "the owner's call",
  "awaits the owner", "not yet approved". The question goes to the owner directly
  (`writing-to-the-owner`), and the pull request opens after the answer.
- **An approval claimed without its source**: a bullet, table row, or paragraph saying the
  owner approved, accepted, decided, agreed, confirmed, wants, said, or told something also
  holds a `YYYY-MM-DD` date and a quotation of at least fifteen characters (`"..."` or a `>`
  line), so a bare "ok" is quoted with the question it answered, or names a decision file
  that records it (`decision N`) and that this pull request does not itself add.
- **A rule change without its record**: a pull request touching `AGENTS.md` or
  `.claude/skills/` holds a dated quote; one whose diff removes or rewrites a line of
  `AGENTS.md` also holds a `Decisions touched:` line naming decision numbers or `none`.

It checks the shape, not the substance: whether a quoted sentence covers the change is for
the reader (`plan-parallel-prs`, section 6). Three ways to run it, each with only the
standard library:

    uv run python scripts/check_pr_body.py              # the branch: .git-pr-body.md and git diff
    uv run python scripts/check_pr_body.py --pr 613     # a pull request, read with gh
    python3 -I scripts/check_pr_body.py --body-file b.md --diff-file d.patch \
        --changed-file AGENTS.md

The first form is the pre-push hook (`.pre-commit-config.yaml`); with no `.git-pr-body.md`
in the worktree it passes and says so, since CI checks the published description. The third
is the `Conventional PR` workflow (`.github/workflows/conventional-pr-check.yml`), which runs
it whenever a pull request opens, its description changes, or a push changes its diff.

Exit status 0 when every rule holds, 1 when at least one is broken (each problem printed on
its own line), 2 for a usage error. When the environment variable `GITHUB_OUTPUT` names a
file, the problems are also appended to it as the step output `error_message`, which the
workflow posts as a comment on the pull request.
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import re
import subprocess
import sys
import uuid
from typing import Final

#: What asks the owner something: a question mark, or the phrases of a deferred decision.
OPEN_ITEM: Final = re.compile(
    r"\?|\bopen:|\b(to confirm|unconfirmed|for (the owner|you) to (confirm|decide|judge|settle)"
    r"|the owner'?s call|awaits? the owner|pending the owner"
    r"|not yet (approved|confirmed|decided))\b",
    re.IGNORECASE,
)
#: The heading that starts the Check before merging list, bold or a Markdown heading.
SECTION_HEADING: Final = re.compile(r"^\s*(>\s*)?(\*\*|#+ ?)Check before merging", re.IGNORECASE)
#: A claim that the owner approved something, in the author's words.
APPROVAL_CLAIM: Final = re.compile(
    r"\b(owner[- ]approved|(approved|accepted|confirmed|agreed|decided) by the owner"
    r"|the owner (decided|approved|accepted|agreed|confirmed|wants|said|told))\b",
    re.IGNORECASE,
)
#: A decision file as the source of an approval: the record the owner's words live in.
DECISION_REFERENCE: Final = re.compile(r"\bdecision (\d+)\b", re.IGNORECASE)
DATE: Final = re.compile(r"\b20\d\d-\d\d-\d\d\b")
#: Fifteen characters or more: a one-word answer ("ok", "a.") is quoted with its question.
QUOTE: Final = re.compile(r'"[^"]{15,}"|“[^”]{15,}”|^\s*>\s*\S.{14,}', re.MULTILINE)
#: Quotations, blockquote lines, code spans, and links may hold a `?` that asks nothing.
QUOTED_OR_CODE: Final = re.compile(
    r'"[^"]*"|“[^”]*”|`[^`]*`|https?://\S+|^\s*>[^\n]*', re.DOTALL | re.MULTILINE
)
#: The line a rule rewrite must carry: the decisions it touches, or none.
DECISIONS_TOUCHED: Final = re.compile(
    r"Decisions touched:\s*(none|\d+(\s*,\s*\d+)*)", re.IGNORECASE
)
#: A deleted diff line that is not the `---` file header and not blank.
DELETED_LINE: Final = re.compile(r"^-(?!--)\s*\S")
RULE_FILES: Final = ("AGENTS.md", ".claude/skills/")
BODY_FILE: Final = pathlib.Path(".git-pr-body.md")
STANDARD: Final = "docs/dev/pull-requests.md"


def check_section(body: str) -> list[str]:
    """Return each line under Check before merging that asks the owner something.

    Args:
        body: The description's Markdown.

    Returns:
        One line per hit, from each `**Check before merging` heading to the next `## `
        heading. Quotations, blockquote lines, code spans, and links are blanked first, since
        a quoted question or a `?` in a URL asks nothing.
    """
    lines = QUOTED_OR_CODE.sub("", body).splitlines()
    problems = []
    for start, line in enumerate(lines):
        if not SECTION_HEADING.match(line):
            continue
        for later in lines[start:]:
            if later.startswith("## ") and later is not line:
                break
            hit = OPEN_ITEM.search(later)
            if hit:
                problems.append(
                    f"Check before merging asks the owner something ({hit.group(0)!r}): "
                    f"{later.strip()[:100]!r}. Ask the owner directly and open or push the "
                    f"pull request after the answer ({STANDARD})."
                )
    return problems


def units(body: str) -> list[str]:
    """Split the body into the units a claim lives in.

    Args:
        body: The description's Markdown.

    Returns:
        Each table row on its own; each bullet or numbered item with its indented
        continuation and sub-bullets; each prose paragraph (consecutive non-empty lines).
    """
    result: list[str] = []
    current: list[str] = []
    for line in [*body.splitlines(), ""]:
        stripped = line.strip()
        starts_item = stripped.startswith(("- ", "* ", "|")) or re.match(r"\d+\. ", stripped)
        indented = line.startswith((" ", "\t"))
        if not stripped or (starts_item and not indented) or stripped.startswith("|"):
            if current:
                result.append("\n".join(current))
            current = [line] if stripped else []
        else:
            current.append(line)
    if current:
        result.append("\n".join(current))
    return result


def is_dated_quote(text: str) -> bool:
    """Whether the text holds both a date and a quotation.

    Args:
        text: A unit of the body, or the whole body.

    Returns:
        True when `DATE` and `QUOTE` both match somewhere in the text.
    """
    return bool(DATE.search(text) and QUOTE.search(text))


def _cites_existing_decision(unit: str, changed: list[str]) -> bool:
    """Whether the unit names a decision file that this pull request does not add.

    Args:
        unit: A bullet, row, or paragraph of the body.
        changed: The paths the pull request changes.

    Returns:
        True for a `decision N` whose file `docs/dev/decisions/NNNN-...` is not among the
        changed paths: a decision written in the same pull request records nothing yet.
    """
    return any(
        not any(path.startswith(f"docs/dev/decisions/{int(number):04d}-") for path in changed)
        for number in DECISION_REFERENCE.findall(unit)
    )


def check_claims(body: str, changed: list[str]) -> list[str]:
    """Return each unit that claims an approval without its source.

    Args:
        body: The description's Markdown.
        changed: The paths the pull request changes.

    Returns:
        One line per bullet, row, or paragraph whose own words (outside quotations and code)
        match `APPROVAL_CLAIM` while the unit holds neither a dated quotation of fifteen
        characters or more nor a reference to a decision file this pull request does not add.
    """
    problems = []
    for unit in units(body):
        hit = APPROVAL_CLAIM.search(QUOTED_OR_CODE.sub("", unit))
        if hit and not (is_dated_quote(unit) or _cites_existing_decision(unit, changed)):
            problems.append(
                f"An approval is claimed ({hit.group(0)!r}) without the owner's words quoted "
                f"with their date, or the decision that records them, in the same bullet, "
                f"row, or paragraph: {unit.strip()[:100]!r} (AGENTS.md, an approval is a "
                f"quote)."
            )
    return problems


def agents_md_deletions(diff: str) -> bool:
    """Whether the diff deletes a non-blank line of AGENTS.md (a rule dropped or rewritten).

    Args:
        diff: A unified diff of the branch against its base.

    Returns:
        True when a `-` line inside the AGENTS.md hunks holds text.
    """
    in_agents = False
    for line in diff.splitlines():
        if line.startswith("diff --git "):
            in_agents = line.endswith(" b/AGENTS.md")
        elif in_agents and DELETED_LINE.match(line):
            return True
    return False


def check_rule_change(body: str, changed: list[str], diff: str) -> list[str]:
    """Return the problems of a pull request that edits AGENTS.md or a skill.

    Args:
        body: The description's Markdown.
        changed: The paths the pull request changes.
        diff: Its unified diff.

    Returns:
        A line when a rule file changes and the body holds no dated quote, and a line when
        AGENTS.md loses a line and the body holds no `Decisions touched:` line. Empty when no
        rule file changes.
    """
    rule_files = [f for f in changed if f == RULE_FILES[0] or f.startswith(RULE_FILES[1])]
    if not rule_files:
        return []
    problems = []
    if not is_dated_quote(body):
        problems.append(
            f"The pull request changes a rule ({', '.join(rule_files)}) without the owner's "
            f"words quoted with their date (AGENTS.md, an approval is a quote)."
        )
    if agents_md_deletions(diff) and not DECISIONS_TOUCHED.search(body):
        problems.append(
            "The pull request removes or rewrites a line of AGENTS.md without a 'Decisions "
            "touched: <numbers or none>' line (AGENTS.md, Workflow)."
        )
    return problems


def find_problems(body: str, changed: list[str], diff: str = "") -> list[str]:
    """Return every problem of the description, in the order the rules are listed.

    Args:
        body: The description's Markdown.
        changed: The paths the pull request changes.
        diff: Its unified diff; empty when unknown, which skips the AGENTS.md deletion rule.
    """
    return (
        check_section(body) + check_claims(body, changed) + check_rule_change(body, changed, diff)
    )


def _run(command: list[str]) -> str:
    return subprocess.run(command, check=True, capture_output=True, text=True).stdout  # noqa: S603 - fixed argv, no shell


def _from_gh(number: int) -> tuple[str, list[str], str]:
    data = json.loads(_run(["gh", "pr", "view", str(number), "--json", "body,files"]))
    diff = _run(["gh", "pr", "diff", str(number)])
    return str(data["body"]), [str(f["path"]) for f in data["files"]], diff


def _base() -> str:
    """The branch this one is measured against: its pull request's base, or `origin/main`.

    Returns:
        `origin/<base branch>` from `gh pr view` when the branch has a pull request (a
        stacked one is diffed against the PR below it, not `main`), else `origin/main`.
    """
    try:
        view = _run(["gh", "pr", "view", "--json", "baseRefName"])
    except subprocess.CalledProcessError:
        return "origin/main"
    return "origin/" + str(json.loads(view)["baseRefName"])


def _from_branch() -> tuple[str, list[str], str]:
    body = BODY_FILE.read_text()
    base = _base()
    diff = _run(["git", "diff", f"{base}...HEAD"])
    names = _run(["git", "diff", "--name-only", f"{base}...HEAD"])
    return body, [line for line in names.splitlines() if line], diff


def main(argv: list[str] | None = None) -> int:
    """Check one description and print its problems; the exit status is the verdict.

    Args:
        argv: The command line without the program name; `None` reads `sys.argv`.

    Returns:
        0 when every rule holds, 1 when at least one problem was printed.
    """
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--pr", type=int, help="the pull request number, read with gh")
    source.add_argument("--body-file", type=pathlib.Path, help="a file holding the body")
    parser.add_argument(
        "--changed-file", action="append", default=[], help="a changed path (with --body-file)"
    )
    parser.add_argument(
        "--diff-file", type=pathlib.Path, help="the unified diff (with --body-file)"
    )
    args = parser.parse_args(argv)
    if args.pr is not None:
        body, changed, diff = _from_gh(args.pr)
    elif args.body_file is not None:
        body, changed = args.body_file.read_text(), args.changed_file
        diff = args.diff_file.read_text() if args.diff_file else ""
    elif BODY_FILE.exists():
        body, changed, diff = _from_branch()
    else:
        sys.stdout.write(
            f"No {BODY_FILE} in this worktree; the Conventional PR workflow checks the "
            f"published description.\n"
        )
        return 0
    problems = find_problems(body, changed, diff)
    sys.stdout.writelines(f"{problem}\n" for problem in problems)
    output = os.environ.get("GITHUB_OUTPUT")
    if problems and output:
        delimiter = uuid.uuid4().hex
        with pathlib.Path(output).open("a", encoding="utf-8") as handle:
            handle.write(f"error_message<<{delimiter}\n" + "\n".join(problems) + f"\n{delimiter}\n")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
