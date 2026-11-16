"""The rows a decoder could not read whole, taken out before the library publishes a batch.

Every decoder (a REST page, a flat-file block, a WebSocket frame) returns every row it read,
with the rows it could not read whole marked by fault (`_massive.RowMarks`). A row whose key
cannot be read (`RowFault.UNREADABLE_KEYS`) cannot be placed in a table, so the transports
take it out here and count it as a dropped row of the payload's drift, which the session's
`DriftRegistry` logs. A row marked for any other fault stays, with its null.

Pure Arrow and plain values: no lock, no thread, and no Deephaven call, so any reader thread
may call it.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa

from deephaven_massive._massive import DriftReport, RowFault

if TYPE_CHECKING:
    from deephaven_massive._massive import RowMarks

__all__ = ["without_unread_keys"]


def without_unread_keys(
    rows: pa.RecordBatch, marks: RowMarks
) -> tuple[pa.RecordBatch, DriftReport]:
    """Take the rows whose key cannot be read out of a decoded batch.

    Args:
        rows: Every row a decoder read from one payload.
        marks: The decoder's marks of those rows.

    Returns:
        The other rows, in order; and a `DriftReport` of the rows taken out
        (`dropped_rows`, with the first one's reason), empty when there were none.
    """
    unread = marks.get(RowFault.UNREADABLE_KEYS)
    if unread is None:
        return rows, DriftReport()
    taken = set(unread.rows)
    kept = [row for row in range(rows.num_rows) if row not in taken]
    dropped = DriftReport(dropped_rows=len(unread.rows), dropped_reason=unread.reasons[0])
    return rows.take(pa.array(kept, pa.int64())), dropped
