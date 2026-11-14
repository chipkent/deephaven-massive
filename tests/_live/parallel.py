"""Which tests of a run are live, and the refusal to run any of them on parallel workers.

The live tests run in one process. They share each API key's rate limits, which a second
process would spend behind the first one's back, and they write one evidence log and one
summary, which only a single process keeps whole. A run on pytest-xdist workers (`-n`) that
selects even one live test is therefore refused before any test starts.

The refusal is decided from the tests the run actually selected, never from its `-m`
expression: an expression can select live tests in ways no reading of it foresees (`-m ""`
selects everything, `-m "integration and needs"` a subset). The xdist controller sees only the
ids of the tests each worker selected, not their markers, so a live test is recognized by its
file: the `integration` marker is on exactly the tests of `*_integration.py` files, which
`tests/conftest.py` checks on every collected test before any is deselected.

The module is also the pytest plugin that applies the refusal: `tests/conftest.py` registers
it, and its xdist hook calls `refusal` once each worker has collected. It imports neither the
library nor Deephaven, so a pytest run can load it with `-p` and no JVM. Every other function
is pure.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Final

import pytest

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

INTEGRATION_FILE_SUFFIX: Final = "_integration.py"
"""How the name of a file of live tests ends, such as `test_stocks_integration.py`."""

PLUGIN_NAME: Final = "live-parallel-refusal"
"""The name `tests/conftest.py` registers this module under as a pytest plugin."""


def is_live(nodeid: str) -> bool:
    """Tell whether a test is a live test, from its pytest id.

    Args:
        nodeid: The test's id, its file path and then its name, separated by `::`, such as
            `"tests/namespaces/test_stocks_integration.py::test_trades"`.

    Returns:
        True when the test's file is a file of live tests.
    """
    return nodeid.split("::", 1)[0].endswith(INTEGRATION_FILE_SUFFIX)


def refusal(nodeids: Iterable[str]) -> str | None:
    """Describe why a parallel run of the selected tests is refused, if it is.

    Args:
        nodeids: The ids of the tests a worker selected, after `-m`, `-k`, and every other
            deselection.

    Returns:
        The message that ends the run when any selected test is live, naming how many are and
        the first of them; None when none is, and the run may go on in parallel.
    """
    live = [nodeid for nodeid in nodeids if is_live(nodeid)]
    if not live:
        return None
    return (
        "live tests run in one process, since they share the key's rate limits and one "
        f"evidence log, and this run selects {len(live)} of them (the first is {live[0]}): "
        "drop -n, or pass -n 0"
    )


@pytest.hookimpl(optionalhook=True)
def pytest_xdist_node_collection_finished(ids: Sequence[str]) -> None:
    """End a parallel run that selected a live test, before any test starts.

    pytest-xdist calls this in its controller each time a worker has collected, with the ids
    of the tests that worker selected after `-m`, `-k`, and every other deselection. Each
    worker's report is checked as it arrives, and the controller hands out no test until every
    worker has reported, so a refusal always comes before any test is scheduled. The decision
    rests on the selection, not on the `-m` expression, so no expression can slip a live test
    past it. Without pytest-xdist, or with `-n 0`, the hook is never called.

    A refused run ends through `pytest.exit` with pytest's usage-error status, 4, and a message
    naming how many live tests the worker selected and the first of them.

    Args:
        ids: The ids of the tests the worker selected.
    """
    message = refusal(ids)
    if message is not None:
        pytest.exit(message, returncode=pytest.ExitCode.USAGE_ERROR)
