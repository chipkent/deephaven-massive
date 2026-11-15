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
  line), so a bare "ok" is quoted with the question it answered, or names a decision
  (`decision N`) whose file is in `docs/dev/decisions/` and that this pull request does not
  itself add.
- **A rule change without its record**: a pull request touching `AGENTS.md` or
  `.claude/skills/` holds a dated quote; one whose diff removes or rewrites a line of either
  also holds a `Decisions touched:` line naming decision numbers or `none`.

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
#: Paired quotation marks, straight or curly, and a blockquote line: each a quotation.
QUOTATIONS: Final = re.compile(r'"([^"]*)"|“([^”]*)”|^\s*>\s*(\S.*)$', re.MULTILINE)
#: Fifteen characters or more: a one-word answer ("ok", "a.") is quoted with its question.
QUOTE_LENGTH: Final = 15
#: Where the decisions live, relative to the repository root the script runs from.
DECISIONS_DIR: Final = pathlib.Path("docs/dev/decisions")
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
#: The `b/` path of a `diff --git` header, plain or quoted as git quotes non-ASCII names.
DIFF_HEADER: Final = re.compile(
    r'^diff --git (?:"a/.*"|a/.*) (?:"b/(?P<quoted>.*)"|b/(?P<plain>.*))$'
)
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
        section: list[str] = []
        for later in lines[start:]:
            if later.startswith("## ") and later is not line:
                break
            section.append(later)
        # A phrase wrapped across the lines of one item is still that phrase.
        for unit in units("\n".join(section)):
            flat = " ".join(unit.split())
            hit = OPEN_ITEM.search(flat)
            if hit:
                problems.append(
                    f"Check before merging asks the owner something ({hit.group(0)!r}): "
                    f"{flat[:100]!r}. Ask the owner directly and open or push the pull "
                    f"request after the answer ({STANDARD})."
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
    lines = body.splitlines()
    result: list[str] = []
    current: list[str] = []
    for index, line in enumerate(lines):
        stripped = line.strip()
        starts_item = stripped.startswith(("- ", "* ", "|")) or re.match(r"\d+\. ", stripped)
        indented = line.startswith((" ", "\t"))
        if not stripped:
            # A blank line inside a list item (the next text is indented) continues the item.
            following = next((later for later in lines[index + 1 :] if later.strip()), "")
            if current and following.startswith((" ", "\t")):
                continue
            if current:
                result.append("\n".join(current))
            current = []
        elif (starts_item and not indented) or stripped.startswith("|"):
            if current:
                result.append("\n".join(current))
            current = [line]
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
        True when the text holds a `DATE` and a paired quotation or blockquote line of
        `QUOTE_LENGTH` characters or more. Quotation marks are paired in order, so the prose
        between two short quotations never counts as one.
    """
    quotations = ("".join(groups) for groups in QUOTATIONS.findall(text))
    return bool(DATE.search(text)) and any(len(q) >= QUOTE_LENGTH for q in quotations)


def added_paths(diff: str) -> set[str]:
    """Return the paths the diff adds as new files.

    Args:
        diff: A unified diff of the branch against its base.

    Returns:
        Each `b/` path whose header is followed by `new file mode`.
    """
    added: set[str] = set()
    path = ""
    for line in diff.splitlines():
        header = DIFF_HEADER.match(line)
        if header:
            path = header.group("quoted") or header.group("plain")
        elif line.startswith("new file mode") and path:
            added.add(path)
    return added


def cites_existing_decision(unit: str, added: set[str], decisions_dir: pathlib.Path) -> bool:
    """Whether the unit names a decision that exists and this pull request does not add.

    Args:
        unit: A bullet, row, or paragraph of the body.
        added: The paths the pull request adds as new files.
        decisions_dir: The folder of decision files, `NNNN-<slug>.md`.

    Returns:
        True for a `decision N` whose file is in `decisions_dir` and not among `added`: a
        number no file carries records nothing, and neither does a decision written in the
        same pull request.
    """
    for number in DECISION_REFERENCE.findall(unit):
        files = list(decisions_dir.glob(f"{int(number):04d}-*.md"))
        if files and not any(f"docs/dev/decisions/{f.name}" in added for f in files):
            return True
    return False


def check_claims(body: str, diff: str, decisions_dir: pathlib.Path) -> list[str]:
    """Return each unit that claims an approval without its source.

    Args:
        body: The description's Markdown.
        diff: The pull request's unified diff, for the decision files it adds.
        decisions_dir: The folder of decision files.

    Returns:
        One line per bullet, row, or paragraph whose own words (outside quotations and code)
        match `APPROVAL_CLAIM` while the unit holds neither a dated quotation of fifteen
        characters or more nor a reference to an existing decision this pull request does not
        add.
    """
    problems = []
    added = added_paths(diff)
    for unit in units(body):
        hit = APPROVAL_CLAIM.search(QUOTED_OR_CODE.sub("", unit))
        if hit and not (
            is_dated_quote(unit) or cites_existing_decision(unit, added, decisions_dir)
        ):
            problems.append(
                f"An approval is claimed ({hit.group(0)!r}) without the owner's words quoted "
                f"with their date, or the decision that records them, in the same bullet, "
                f"row, or paragraph: {unit.strip()[:100]!r} (AGENTS.md, an approval is a "
                f"quote)."
            )
    return problems


def rule_deletions(diff: str) -> bool:
    """Whether the diff deletes a non-blank line of AGENTS.md or a skill (a rule rewritten).

    Args:
        diff: A unified diff of the branch against its base.

    Returns:
        True when a `-` line inside the hunks of a rule file holds text.
    """
    in_rule_file = False
    for line in diff.splitlines():
        header = DIFF_HEADER.match(line)
        if header:
            path = header.group("quoted") or header.group("plain")
            in_rule_file = path == RULE_FILES[0] or path.startswith(RULE_FILES[1])
        elif in_rule_file and DELETED_LINE.match(line):
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
        a rule file loses a line and the body holds no `Decisions touched:` line. Empty when
        no rule file changes.
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
    if rule_deletions(diff) and not DECISIONS_TOUCHED.search(body):
        problems.append(
            "The pull request removes or rewrites a line of AGENTS.md or a skill without a "
            "'Decisions touched: <numbers or none>' line (AGENTS.md, Workflow)."
        )
    return problems


def find_problems(
    body: str, changed: list[str], diff: str = "", *, decisions_dir: pathlib.Path = DECISIONS_DIR
) -> list[str]:
    """Return every problem of the description, in the order the rules are listed.

    Args:
        body: The description's Markdown.
        changed: The paths the pull request changes.
        diff: Its unified diff; empty when unknown, which skips the rule-deletion check
            and treats every cited decision as one this pull request did not add.
        decisions_dir: The folder of decision files; absent, no cited decision counts.

    Returns:
        The problems of each rule in turn: the Check before merging items, the claims, the
        rule change.
    """
    return (
        check_section(body)
        + check_claims(body, diff, decisions_dir)
        + check_rule_change(body, changed, diff)
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
