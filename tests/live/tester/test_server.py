"""Tests of the tester's server sizing (`tests/_live/tester/server.py`)."""

from __future__ import annotations

import pytest

from tests._live.live_run.server import PARENT_HEAP
from tests._live.tester import server


def test_examples_gets_the_heap_its_tables_need() -> None:
    assert server.jvm_args(["examples", "--folder", "x"]) == [f"-Xmx{server.EXAMPLES_HEAP >> 20}m"]


@pytest.mark.parametrize("argv", [[], ["check"], ["bug-report", "--folder", "x"]])
def test_every_other_command_gets_the_small_heap(argv: list[str]) -> None:
    assert server.jvm_args(argv) == [f"-Xmx{PARENT_HEAP >> 20}m"]
