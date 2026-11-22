"""The check that lets a pull request changing only docstrings and comments run one Python.

The script is run by path, so the tests load it from `scripts/`. Each test writes a base
checkout and a head checkout of a module and asks the script about the change, as CI does.
"""

from __future__ import annotations

import importlib.util
import pathlib
import runpy
from typing import TYPE_CHECKING, Protocol

import pytest

if TYPE_CHECKING:
    from types import ModuleType

_SCRIPT = pathlib.Path(__file__).resolve().parents[2] / "scripts" / "docstring_only_change.py"

#: A module in the library's style: a module docstring, a class, a method, and a comment.
BASE = '''"""Read a ticker's bars."""

from __future__ import annotations


class Bars:
    """The bars of one ticker."""

    def read(self, ticker: str) -> list[str]:
        """Read the bars.

        Raises:
            ValueError: The ticker is empty.
        """
        # Massive answers an empty ticker with every ticker's bars.
        if not ticker:
            raise ValueError("no ticker")
        return [ticker]


async def follow(ticker: str) -> str:
    """Follow the bars."""
    return ticker
'''

#: `BASE` with one method's docstring reworded.
REWORDED = BASE.replace("Read the bars.", "Read the bars of the day.")


class Ask(Protocol):
    """Run the script on a head version of `src/bars.py`, as CI runs it."""

    def __call__(self, head: str | None, *, event: str = ...) -> tuple[str, str]:
        """Write `head` (or delete the file, for None) and run the script on it.

        Returns:
            What the script printed on standard output and on standard error.
        """
        ...


@pytest.fixture(scope="module")
def script() -> ModuleType:
    """The script as a module.

    Returns:
        `scripts/docstring_only_change.py`, loaded by path as CI runs it.
    """
    spec = importlib.util.spec_from_file_location("docstring_only_change", _SCRIPT)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def base(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> pathlib.Path:
    """A base checkout holding `BASE`, with the current directory as the head checkout.

    Returns:
        The base checkout's directory.
    """
    checkout = tmp_path / "base"
    (checkout / "src").mkdir(parents=True)
    (checkout / "src" / "bars.py").write_text(BASE)
    head = tmp_path / "head"
    (head / "src").mkdir(parents=True)
    monkeypatch.chdir(head)
    return checkout


@pytest.fixture
def ask(script: ModuleType, base: pathlib.Path, capsys: pytest.CaptureFixture[str]) -> Ask:
    """The script, run on a head version of `src/bars.py` against `BASE`.

    Returns:
        A function that writes the head version, runs the script, and returns its output.
    """

    def run(head: str | None, *, event: str = "pull_request") -> tuple[str, str]:
        if head is not None:
            pathlib.Path("src/bars.py").write_text(head)
        assert script.main([event, str(base), "src/bars.py"]) == 0
        captured = capsys.readouterr()
        return captured.out, captured.err

    return run


@pytest.mark.parametrize(
    "head",
    [
        pytest.param(REWORDED, id="method"),
        pytest.param(BASE.replace("Read a ticker's", "Read one ticker's"), id="module"),
        pytest.param(BASE.replace("The bars of one", "Every bar of one"), id="class"),
        pytest.param(BASE.replace("Follow the bars.", "Follow the bars live."), id="async"),
        pytest.param(
            BASE.replace(
                "            ValueError: The ticker is empty.\n",
                "            ValueError: The ticker is empty.\n            KeyError: Never.\n",
            ),
            id="raises-section-grows",
        ),
        pytest.param(BASE.replace('    """Follow the bars."""\n', ""), id="docstring-removed"),
    ],
)
def test_a_docstring_change_runs_one_python(ask: Ask, head: str) -> None:
    assert ask(head) == ("true\n", "src/bars.py: changes only docstrings and comments\n")


def test_a_comment_change_runs_one_python(ask: Ask) -> None:
    head = BASE.replace("# Massive answers", "# Massive, asked for nothing, answers")
    assert ask(head + "# A trailing comment.\n")[0] == "true\n"


@pytest.mark.parametrize(
    "head",
    [
        pytest.param(BASE.replace('"no ticker"', '"empty ticker"'), id="string"),
        pytest.param(BASE.replace("return [ticker]", "return [ticker, ticker]"), id="code"),
        pytest.param(
            BASE.replace('"""The bars of one ticker."""', '"""The bars."""\n    size = 1'),
            id="docstring-and-code",
        ),
        pytest.param(
            BASE.replace("return ticker\n", 'return ticker\n    "not a docstring"\n'),
            id="string-after-the-first-statement",
        ),
    ],
)
def test_a_code_change_runs_everything(ask: Ask, head: str) -> None:
    assert ask(head) == ("false\n", "src/bars.py: changes code\n")


def test_a_pragma_comment_change_runs_everything(ask: Ask) -> None:
    head = BASE.replace("return [ticker]", "return [ticker]  # pragma: no cover")
    assert ask(head) == ("false\n", "src/bars.py: changes a line that mentions pragma\n")


def test_one_code_change_among_docstring_changes_runs_everything(
    script: ModuleType, base: pathlib.Path, capsys: pytest.CaptureFixture[str]
) -> None:
    (base / "src" / "other.py").write_text(BASE)
    pathlib.Path("src/bars.py").write_text(REWORDED)
    pathlib.Path("src/other.py").write_text(BASE.replace("return ticker\n", "return ''\n"))
    assert script.main(["pull_request", str(base), "src/bars.py", "src/other.py"]) == 0
    assert capsys.readouterr() == (
        "false\n",
        "src/bars.py: changes only docstrings and comments\nsrc/other.py: changes code\n",
    )


def test_a_base_that_does_not_parse_runs_everything(ask: Ask, base: pathlib.Path) -> None:
    (base / "src" / "bars.py").write_text(BASE + "def (\n")
    out, err = ask(BASE)
    assert out == "false\n"
    assert err.startswith("src/bars.py: does not parse: ")


def test_a_head_that_does_not_parse_runs_everything(ask: Ask) -> None:
    out, err = ask(BASE + "def (\n")
    assert out == "false\n"
    assert err.startswith("src/bars.py: does not parse: ")


def test_a_new_file_runs_everything(ask: Ask, base: pathlib.Path) -> None:
    (base / "src" / "bars.py").unlink()
    assert ask(BASE) == ("false\n", "src/bars.py: is missing from the base commit\n")


def test_a_deleted_file_runs_everything(ask: Ask) -> None:
    assert ask(None) == ("false\n", "src/bars.py: is missing from the head commit\n")


@pytest.mark.parametrize("event", ["push", "schedule", "workflow_dispatch", "workflow_call"])
def test_a_run_after_merging_runs_every_python(ask: Ask, event: str) -> None:
    assert ask(REWORDED, event=event) == (
        "false\n",
        f"a {event} run checks every Python version\n",
    )


def test_no_file_runs_everything(
    script: ModuleType, base: pathlib.Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert script.main(["pull_request", str(base)]) == 0
    assert capsys.readouterr() == ("false\n", "no Python file was given\n")


@pytest.mark.parametrize("argv", [[], ["pull_request"]])
def test_missing_arguments_print_no_answer(
    script: ModuleType, argv: list[str], capsys: pytest.CaptureFixture[str]
) -> None:
    assert script.main(argv) == 2
    out, err = capsys.readouterr()
    assert out == ""
    assert err.startswith("usage: ")


def test_a_file_that_is_not_utf8_runs_everything(ask: Ask) -> None:
    pathlib.Path("src/bars.py").write_bytes(b"\xff\xfe")
    out, err = ask(None)
    assert out == "false\n"
    assert err.startswith("src/bars.py: does not parse: ")


def test_a_changed_encoding_declaration_runs_everything(ask: Ask, base: pathlib.Path) -> None:
    # The same bytes mean another string once the declared encoding changes.
    (base / "src" / "bars.py").write_bytes(b'# -*- coding: latin-1 -*-\nS = "\xc3\xa9"\n')
    pathlib.Path("src/bars.py").write_bytes(b'# -*- coding: utf-8 -*-\nS = "\xc3\xa9"\n')
    assert ask(None) == ("false\n", "src/bars.py: changes code\n")


def test_run_by_path_prints_the_answer(
    base: pathlib.Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    pathlib.Path("src/bars.py").write_text(REWORDED)
    monkeypatch.setattr("sys.argv", [str(_SCRIPT), "pull_request", str(base), "src/bars.py"])
    with pytest.raises(SystemExit) as exited:
        runpy.run_path(str(_SCRIPT), run_name="__main__")
    assert exited.value.code == 0
    assert capsys.readouterr().out == "true\n"
