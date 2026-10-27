"""A fake of the GitHub command line, `gh`, for the drift workflow's issue script.

`scripts/spec-drift-issue.sh` keeps the standing drift issue through `gh`. The tests put a
`gh` on `PATH` that runs this module as a program, so the script runs unchanged against an
in-memory repository and nothing reaches GitHub.

`FakeGh` holds the repository's labels and issues in a JSON state file, which `install` writes
and every run of the program reads and rewrites; a test seeds it before the script runs and
reads `labels`, `issues`, and `calls` afterwards. The program answers exactly the commands the
script uses, with the output shapes real `gh` gives for `--json` (issue states `OPEN` and
`CLOSED`, newest issue first, at most `--limit` of them), and fails on anything else, so a new
command in the script fails its tests until the fake learns it:

- `label list --search S --limit 1000 --json name`;
- `label create NAME --color C --description D` (a name that exists in any case fails, as
  GitHub's label names compare without regard to case);
- `issue list --label L --state all --limit 1000 --json number,title,state`;
- `issue create --title T --label L --body-file F`;
- `issue view N --json body`, `issue edit N --body-file F`, `issue reopen N`, and
  `issue comment N --body B` (recorded in the issue's `comments`).

Like GitHub, it stores a body without its trailing newlines, so the script must compare
bodies as text, not byte for byte.

Like `gh`, it refuses to run without `GH_TOKEN` and `GH_REPO`, and a command can be made to
fail (`fail`) to test that the script stops on it.
"""

from __future__ import annotations

import json
import os
import stat
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Sequence

_STATE_VARIABLE = "FAKE_GH_STATE"


class FakeGh:
    """An in-memory repository answered by a fake `gh` program on `PATH`."""

    def __init__(self, directory: Path) -> None:
        """Prepare a fake in `directory`; `install` writes the program and its state.

        Args:
            directory: An empty directory of the test's own.
        """
        self.bin = directory / "bin"
        self.state = directory / "gh-state.json"
        self.labels: list[str] = []
        self.issues: list[dict[str, Any]] = []
        self.failing: list[str] = []
        self.calls: list[list[str]] = []

    def issue(
        self, title: str, *, labels: Sequence[str], state: str = "OPEN", body: str = ""
    ) -> int:
        """Add an existing issue.

        Returns:
            Its number.
        """
        number = len(self.issues) + 1
        self.issues.append(
            {
                "number": number,
                "title": title,
                "state": state,
                "labels": list(labels),
                "body": _stored(body),
                "comments": [],
            }
        )
        return number

    def fail(self, *command: str) -> None:
        """Make every call whose arguments start with `command` fail with status 1."""
        self.failing.append(" ".join(command))

    def install(self) -> dict[str, str]:
        """Write the program and the state, and return the environment that runs them.

        Returns:
            The current environment with the fake `gh` first on `PATH`, `GH_TOKEN` and
            `GH_REPO` set, and the state file named.
        """
        self.bin.mkdir(parents=True, exist_ok=True)
        program = self.bin / "gh"
        program.write_text(f'#!/bin/sh\nexec "{sys.executable}" "{Path(__file__)}" "$@"\n')
        program.chmod(program.stat().st_mode | stat.S_IXUSR)
        self.save()
        return {
            **os.environ,
            "PATH": f"{self.bin}{os.pathsep}{os.environ['PATH']}",
            "GH_TOKEN": "fake-token",
            "GH_REPO": "owner/repo",
            _STATE_VARIABLE: str(self.state),
        }

    def load(self) -> None:
        """Read back what the program changed: `labels`, `issues`, and `calls`."""
        state = json.loads(self.state.read_text())
        self.labels, self.issues = state["labels"], state["issues"]
        self.failing, self.calls = state["failing"], state["calls"]

    def save(self) -> None:
        """Write `labels`, `issues`, and `calls` to the state file."""
        state = {
            "labels": self.labels,
            "issues": self.issues,
            "failing": self.failing,
            "calls": self.calls,
        }
        self.state.write_text(json.dumps(state))


def _options(arguments: Sequence[str]) -> dict[str, str]:
    # Pair each `--name` with the value after it.
    if len(arguments) % 2 or not all(a.startswith("--") for a in arguments[::2]):
        raise SystemExit(f"fake gh: cannot read the options {list(arguments)}")
    return dict(zip(arguments[::2], arguments[1::2], strict=True))


def _expect(options: dict[str, str], **expected: str) -> None:
    # Require the options the script must pass, and allow only the known free-form ones.
    wanted = {f"--{name.replace('_', '-')}": value for name, value in expected.items()}
    found = {name: options.get(name) for name in wanted}
    if found != wanted or options.keys() - wanted.keys() - {"--search", "--title", "--label"}:
        raise SystemExit(f"fake gh: unexpected options {options}, wanted {wanted}")


def _stored(body: str) -> str:
    # GitHub keeps a body without its trailing newlines.
    return body.rstrip("\n")


def _label(fake: FakeGh, arguments: list[str]) -> str:
    # Answer one `label` command, changing `fake`, and return what it prints.
    match arguments:
        case ["list", *rest]:
            options = _options(rest)
            _expect(options, limit="1000", json="name")
            search = options["--search"]
            found = [name for name in fake.labels if search.lower() in name.lower()]
            return json.dumps([{"name": name} for name in found])
        case ["create", name, *rest]:
            _options(rest)
            if name.lower() in {label.lower() for label in fake.labels}:
                raise SystemExit(f"label with name {name!r} already exists")
            fake.labels.append(name)
            return ""
    raise SystemExit(f"fake gh: unknown command label {arguments}")


def _listed(fake: FakeGh, options: dict[str, str]) -> str:
    # The issues `issue list` prints: newest first, at most `--limit` of them.
    _expect(options, state="all", limit="1000", json="number,title,state")
    found = [i for i in reversed(fake.issues) if options["--label"] in i["labels"]]
    found = found[: int(options["--limit"])]
    return json.dumps([{k: i[k] for k in ("number", "title", "state")} for i in found])


def _issue(fake: FakeGh, arguments: list[str]) -> str:
    # Answer one `issue` command, changing `fake`, and return what it prints.
    output = ""
    match arguments:
        case ["list", *rest]:
            output = _listed(fake, _options(rest))
        case ["create", *rest]:
            options = _options(rest)
            number = fake.issue(options["--title"], labels=[options["--label"]])
            fake.issues[-1]["body"] = _stored(Path(options["--body-file"]).read_text())
            output = f"https://github.com/owner/repo/issues/{number}"
        case ["view", number, "--json", "body"]:
            output = json.dumps({"body": fake.issues[int(number) - 1]["body"]})
        case ["edit", number, "--body-file", path]:
            fake.issues[int(number) - 1]["body"] = _stored(Path(path).read_text())
        case ["comment", number, "--body", text]:
            fake.issues[int(number) - 1]["comments"].append(text)
        case ["reopen", number]:
            fake.issues[int(number) - 1]["state"] = "OPEN"
        case _:
            raise SystemExit(f"fake gh: unknown command issue {arguments}")
    return output


def _run(fake: FakeGh, arguments: list[str]) -> str:
    # Answer one command, changing `fake`, and return what it prints.
    match arguments:
        case ["label", *rest]:
            return _label(fake, rest)
        case ["issue", *rest]:
            return _issue(fake, rest)
    raise SystemExit(f"fake gh: unknown command {arguments}")


def main(arguments: list[str]) -> int:
    """Run the fake `gh` program on the state file `FAKE_GH_STATE` names.

    Returns:
        The exit status: 0, or 1 for a command made to fail.
    """
    if not os.environ.get("GH_TOKEN") or not os.environ.get("GH_REPO"):
        sys.stderr.write("fake gh: GH_TOKEN and GH_REPO must be set\n")
        return 4
    fake = FakeGh(Path(os.environ[_STATE_VARIABLE]).parent)
    fake.load()
    fake.calls.append(arguments)
    try:
        if any(" ".join(arguments).startswith(command) for command in fake.failing):
            sys.stderr.write(f"fake gh: {arguments} fails as the test asked\n")
            return 1
        sys.stdout.write(_run(fake, arguments) + "\n")
    finally:
        fake.save()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
