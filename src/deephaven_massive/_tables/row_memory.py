"""How much memory a live connection may spend on rows waiting to be published, and how to count it.

A WebSocket connection keeps rows in memory while they wait to be published: between two
flushes, in the flush thread's buffers (`stream.Flusher`), and longer when the update graph
falls behind. A wait that does not end must not take the server's memory, so the rows a
connection keeps waiting are capped at `HELD_BYTES_LIMIT`; past it, the streams they belong to
fail with the reason, rather than the server running out of memory with every table in it.

`frame_bytes` estimates the memory of one frame's decoded rows: the rows of one channel share
one field layout, so the first row's size stands for every row's. It never measures every value
of every row, which would cost the connection's loop thread a call per field on a busy feed.
"""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from collections.abc import Sequence

__all__ = ["HELD_BYTES_LIMIT", "frame_bytes"]

HELD_BYTES_LIMIT: Final = 2**30
"""The most bytes of waiting rows one connection keeps: 1 GiB. Past it, the streams whose rows
wait fail, so a wait that does not end stops growing the server's memory."""


def frame_bytes(rows: Sequence[tuple[object, ...]]) -> int:
    """Estimate the memory one frame's decoded rows take, in bytes.

    The list's own size, plus the first row's size (its tuple and each of its values) for every
    row. Values the rows share, such as small integers, are counted in each row, and a list
    value counts only its own size, not its items'.

    Args:
        rows: The frame's rows of one channel, each a tuple of its fields; at least one.

    Returns:
        The estimate.
    """
    first = rows[0]
    row = sys.getsizeof(first) + sum(sys.getsizeof(value) for value in first)
    return sys.getsizeof(rows) + row * len(rows)
