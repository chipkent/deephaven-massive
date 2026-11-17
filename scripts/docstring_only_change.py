"""Tell CI whether a pull request's Python changes touch only docstrings and comments.

CI's `Decide which jobs the change affects` step asks this script about the Python files a pull
request into `main` changes. When every one of them changes only docstrings and comments, CI
runs the whole suite with coverage on Python 3.14 alone, and skips the build job unless another
changed path needs it; ruff, mypy, and the site build run as the changed paths select them.
The 3.14 suite still runs every check that reads docstrings. One difference remains: Python
3.13 and later strip a docstring's indentation from `__doc__`, so a test reading `__doc__` raw
can see other text on 3.11 and 3.12. The run on `main` after the merge catches that.
Any other run, a push to `main` included, checks every version.

A file changes only docstrings and comments when both commits have it, both versions parse,
their syntax trees are equal once the docstring of the module and of each class and function is
removed (comments are never in the tree), and no added or removed line mentions `pragma`, since
a `# pragma: no cover` comment changes what coverage measures, which can differ by version.

Every other case answers no, so CI runs everything: a run that is not a pull request, no file
given, a file one commit lacks (a new, deleted, or renamed file), a file that does not parse in
either commit (one that is not text in its declared encoding included), and any other change.
Any error exits with a traceback and a nonzero status, which CI also takes as no. CI runs the
base commit's copy of this script, so a pull request that changes the script cannot judge
itself with the change; while the base has no copy, the run fails and CI runs everything.

The head commit's files are read from the current directory, and the base commit's from a
checkout of it in another directory. It prints `true` or `false` on standard output, and on
standard error one line per file saying why. It uses only the standard library and runs on
Python 3.9 and later, since CI runs it with the runner's own Python before installing the
project:

    python3 -I scripts/docstring_only_change.py pull_request ../base src/a.py tests/b.py
"""

from __future__ import annotations

import ast
import difflib
import pathlib
import sys

#: The event whose Python changes may run on one version; every other event runs them all.
PULL_REQUEST = "pull_request"

#: The arguments before the files: the event and the base checkout.
_LEADING_ARGUMENTS = 2


def without_docstrings(source: bytes) -> str:
    """Dump a module's syntax tree without the docstrings of its module, classes, and functions.

    Args:
        source: The module's bytes, decoded as Python decodes a file: by its encoding
            declaration, or else as UTF-8.

    Returns:
        The dump of its tree, without line and column numbers, so text that moves while the
        code stays the same dumps the same.

    Raises:
        SyntaxError: The bytes are not valid Python.
    """
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            first = node.body[0] if node.body else None
            if (
                isinstance(first, ast.Expr)
                and isinstance(first.value, ast.Constant)
                and isinstance(first.value.value, str)
            ):
                del node.body[0]
    return ast.dump(tree)


def touches_pragma(before: bytes, after: bytes) -> bool:
    """Say whether a line added or removed between two versions mentions `pragma`.

    Args:
        before: The base version's bytes.
        after: The head version's bytes.

    Returns:
        True when a changed line holds `pragma`.
    """
    old, new = before.splitlines(), after.splitlines()
    for tag, i1, i2, j1, j2 in difflib.SequenceMatcher(a=old, b=new).get_opcodes():
        if tag != "equal" and any(b"pragma" in line for line in old[i1:i2] + new[j1:j2]):
            return True
    return False


def why_not_only_docstrings(before: bytes, after: bytes) -> str | None:
    """Say why two versions of a module differ in more than docstrings and comments, if they do.

    Args:
        before: The base version's bytes.
        after: The head version's bytes.

    Returns:
        None when they differ only in docstrings and comments, or else the reason they don't.
    """
    try:
        same = without_docstrings(before) == without_docstrings(after)
    except SyntaxError as error:
        return f"does not parse: {error}"
    if not same:
        return "changes code"
    if touches_pragma(before, after):
        return "changes a line that mentions pragma"
    return None


def read(path: pathlib.Path) -> bytes | None:
    """Read a file of one commit's checkout.

    Args:
        path: The file.

    Returns:
        The file's bytes, or None when the checkout has no such file.
    """
    return path.read_bytes() if path.is_file() else None


def decide(event: str, base: pathlib.Path, paths: list[str]) -> bool:
    """Say whether every given file changes only docstrings and comments, and log why.

    Args:
        event: The GitHub event that started the run.
        base: The checkout of the base commit; the head commit's is the current directory.
        paths: The changed Python files, relative to each checkout.

    Returns:
        True only for a pull request whose every file changes only docstrings and comments.
    """
    if event != PULL_REQUEST:
        sys.stderr.write(f"a {event} run checks every Python version\n")
        return False
    if not paths:
        sys.stderr.write("no Python file was given\n")
        return False
    verdict = True
    for path in paths:
        before, after = read(base / path), read(pathlib.Path(path))
        if before is None or after is None:
            reason = f"is missing from the {'base' if before is None else 'head'} commit"
        else:
            reason = why_not_only_docstrings(before, after) or ""
        sys.stderr.write(f"{path}: {reason or 'changes only docstrings and comments'}\n")
        verdict = verdict and not reason
    return verdict


def main(argv: list[str]) -> int:
    """Print `true` or `false` for the event, base checkout, and files in `argv`.

    Args:
        argv: The event, the base commit's checkout, and then the changed Python files.

    Returns:
        0 once the answer is printed, or 2 when the event or the base checkout is missing.
    """
    if len(argv) < _LEADING_ARGUMENTS:
        sys.stderr.write("usage: docstring_only_change.py EVENT BASE_CHECKOUT [PATH...]\n")
        return 2
    event, base, *paths = argv
    sys.stdout.write("true\n" if decide(event, pathlib.Path(base), paths) else "false\n")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
