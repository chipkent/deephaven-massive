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

Every function is pure; `tests/conftest.py` calls `refusal` once each worker has collected.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from collections.abc import Iterable

INTEGRATION_FILE_SUFFIX: Final = "_integration.py"
"""How the name of a file of live tests ends, such as `test_stocks_integration.py`."""


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
