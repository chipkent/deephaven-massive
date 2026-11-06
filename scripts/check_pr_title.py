"""Check a pull request title against the rules of the project's title standard.

The owner squash-merges every pull request, so its title becomes the commit subject on `main`.
The standard for that line lives in the Title section of `docs/dev/pull-requests.md`. This
script checks the part of it a program can check:

- **Type.** The title starts with one of the Conventional Commits types the project uses
  (`feat`, `fix`, `docs`, `test`, `refactor`, `ci`, `chore`), then a colon and one space. The
  only scope allowed is `chore(deps):`, and there is no `!` marker.
- **Lowercase.** The first character after the colon and its space is a lowercase letter,
  since the summary starts with a verb in the imperative mood: "add", not "Add", a number, or
  a name in backticks.
- **No trailing period.**
- **Length.** At most 60 characters, type included. GitHub appends ` (#123)` to the squash
  commit, so a trailing pull request number in that form is not counted.

Whether the title is specific, plain, and about one idea is for reviewers; the script does not
try to judge it.

The `Conventional PR` workflow (`.github/workflows/conventional-pr-check.yml`) runs it on every
pull request whose title is new or changed, with the title as the only argument:

    python3 scripts/check_pr_title.py "fix: stop a failed live table from crashing the server"

It uses only the standard library, so CI runs it with the runner's own Python and no checkout
of the pull request's code.

`--dependabot` relaxes two rules for Dependabot's generated titles, and the workflow passes it
only when `dependabot[bot]` opened the pull request. The length rule is off, since the titles
name the package and both versions. The summary may start with a capital letter, as in
`chore(deps): Bump actions/checkout from 7.0.1 to 7.0.2`. Dependabot infers the case from the
recent commits on `main`, not from any setting in `.github/dependabot.yml`, and this
repository's commits lead it to "Bump". The summary must still start with a letter, and every
other rule applies unchanged.

Exit status: 0 when the title meets every rule, 1 when it breaks at least one (each broken rule
is printed on its own line, with a link to the standard), 2 for a usage error. When the
environment variable `GITHUB_OUTPUT` names a file, as it does in a GitHub Actions step, the
script also appends the printed problems to it as the step output `error_message`, which the
workflow posts as a comment on the pull request.
"""

from __future__ import annotations

import argparse
import os
import pathlib
import re
import sys
import uuid

#: The Conventional Commits types the project uses, in the order the standard lists them.
TYPES = ("feat", "fix", "docs", "test", "refactor", "ci", "chore")

#: The longest title allowed, type included, without GitHub's trailing ` (#123)`.
MAX_LENGTH = 60

#: Where the standard lives; every message links to it.
STANDARD_URL = (
    "https://github.com/chipkent/deephaven-massive/blob/main/docs/dev/pull-requests.md#title"
)

_HEADER = re.compile(r"(?P<type>[A-Za-z]+)(?:\((?P<scope>[^)]*)\))?(?P<bang>!?):(?P<rest>.*)")
_PR_NUMBER = re.compile(r" \(#\d+\)$")


def find_problems(title: str, *, dependabot: bool = False) -> list[str]:
    """Return one message for each rule of the title standard that the title breaks.

    Args:
        title: The pull request title, exactly as GitHub holds it.
        dependabot: Whether the title is Dependabot's generated one. True turns off the
            60-character rule and lets the summary start with a capital letter; the workflow
            passes it only for pull requests that `dependabot[bot]` opened.

    Returns:
        The problems in the order the standard lists its rules, each a full sentence that names
        the rule and links to the standard. An empty list means the title meets every rule the
        script checks.
    """
    problems: list[str] = []
    header = _HEADER.fullmatch(title)
    allowed = ", ".join(f"`{name}:`" for name in TYPES)
    if header is None:
        problems.append(
            "Type: start the title with a type and a colon, as in `fix: stop a failed live table"
            f" from crashing the server`. The types are {allowed}."
        )
    else:
        kind, scope, rest = header["type"], header["scope"], header["rest"]
        if kind not in TYPES:
            problems.append(f"Type: `{kind}` is not one of the types this project uses: {allowed}.")
        if scope is not None and (kind, scope) != ("chore", "deps"):
            problems.append(
                f"Type: the scope `({scope})` is not allowed; the only scope is `chore(deps):`,"
                " for dependency changes."
            )
        if header["bang"]:
            problems.append("Type: drop the `!` after the type; this project does not use it.")
        subject = rest.removeprefix(" ")
        if not rest.startswith(" ") or not subject or subject[0].isspace():
            problems.append(
                "Type: put exactly one space after the colon, then the summary, as in"
                " `docs: explain how history() picks a source`."
            )
        elif not (subject[0].isalpha() if dependabot else subject[0].islower()):
            first_word = subject.split(" ", 1)[0]
            problems.append(
                "Lowercase: start the summary after the colon with a lowercase letter, the first"
                f' letter of a verb in the imperative mood, not "{first_word}".'
            )
    if title.endswith("."):
        problems.append("No trailing period: remove the period at the end of the title.")
    counted = _PR_NUMBER.sub("", title)
    if not dependabot and len(counted) > MAX_LENGTH:
        problems.append(
            f"Length: the title has {len(counted)} characters; the limit is {MAX_LENGTH}, type"
            " included. Name the one change a user meets first and leave the rest to the"
            " description."
        )
    return [f"{problem} See {STANDARD_URL}" for problem in problems]


def _write_step_output(path: str, problems: list[str]) -> None:
    """Append the problems to a GitHub Actions output file as the output `error_message`.

    Args:
        path: The file GitHub Actions names in `GITHUB_OUTPUT`.
        problems: The messages to record, one per line.
    """
    delimiter = f"EOF_{uuid.uuid4().hex}"
    with pathlib.Path(path).open("a", encoding="utf-8") as output:
        output.write(f"error_message<<{delimiter}\n")
        output.writelines(f"- {problem}\n" for problem in problems)
        output.write(f"{delimiter}\n")


def main(argv: list[str] | None = None) -> int:
    """Check the title given on the command line and report each broken rule.

    Args:
        argv: The arguments after the program name; `None` reads `sys.argv`.

    Returns:
        The exit status: 0 when the title meets every rule, 1 when it breaks at least one.
        `argparse` exits with status 2 on a usage error before this returns.
    """
    parser = argparse.ArgumentParser(
        description="Check a pull request title against the title standard."
    )
    parser.add_argument("title", help="the pull request title")
    parser.add_argument(
        "--dependabot",
        action="store_true",
        help="the title is Dependabot's: skip the length limit and allow a capital first letter",
    )
    args = parser.parse_args(argv)
    problems = find_problems(args.title, dependabot=args.dependabot)
    sys.stdout.writelines(f"{problem}\n" for problem in problems)
    output_path = os.environ.get("GITHUB_OUTPUT")
    if problems and output_path:
        _write_step_output(output_path, problems)
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
