"""The problems a live check found: the rows of `m.problems()`, read once every one shows.

A call queues its problems for the session's writer thread, and a row reaches `m.problems()`
only on the update cycle after the writer adds it. A check that reads the table right after
`fetch(wait=True)` returns can miss the call's last problem, and under `IGNORE`, whose causes
log only at DEBUG, nothing else would show it. `problem_lines` waits for the queued rows
first, so a check counts every problem its calls caused.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Final

if TYPE_CHECKING:
    from collections.abc import Callable

    from deephaven.table import Table

    from deephaven_massive import MassiveSession

PROBLEMS_SHOWN: Final = 30.0
"""The most seconds `problem_lines` waits for the queued problems to show."""


def problem_lines(
    session: MassiveSession, table_rows: Callable[[Table], list[dict[str, Any]]]
) -> list[str]:
    """Return one line per row of the session's `problems()`, once every queued row shows.

    Args:
        session: The session; not on an update-graph thread, and not holding the update
            graph's lock, under which no cycle runs.
        table_rows: Reads a table's rows.

    Returns:
        Lines such as `problem noncanonical_symbols msft : ...`: the cause, the symbol and
        field (empty when null), and the detail.
    """
    shown = session._core.diagnostics.wait_until_shown(PROBLEMS_SHOWN)
    assert shown, f"the session's problems did not show within {PROBLEMS_SHOWN:.0f} s"
    return [
        f"problem {r['Cause']} {r['Symbol'] or ''} {r['Field'] or ''}: {r['Detail']}"
        for r in table_rows(session.problems())
    ]
