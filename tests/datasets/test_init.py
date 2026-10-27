"""Tests for the `_datasets` package itself, whose `__init__.py` holds only a docstring."""

import ast
from pathlib import Path

from deephaven_massive import _datasets


def test_package_init_holds_only_a_docstring() -> None:
    module = ast.parse(Path(_datasets.__file__).read_text())
    assert ast.get_docstring(module)
    assert len(module.body) == 1
