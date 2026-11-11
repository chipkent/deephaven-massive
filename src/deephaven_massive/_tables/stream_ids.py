"""The process's one allocator of stream ids, the `StreamId` of every `m.streams()` row.

Stream ids are minted here, not by a transport, so a WebSocket stream is registered before it
opens. Every session of the process draws from the one counter, so no two rows of any
session's `m.streams()`, and no two call ids in the log, ever share an id.

The counter has a leaf lock of its own: the session module's registration lock is taken before
a session's `registry_lock`, and ids are minted under `registry_lock`, so sharing the
registration lock would invert that order.
"""

from __future__ import annotations

import itertools
import threading

__all__ = ["next_stream_id"]

_stream_id_lock = threading.Lock()
_stream_numbers = itertools.count(1)


def next_stream_id() -> int:
    """Return a new process-unique stream id, the `StreamId` of an `m.streams()` row.

    WebSocket streams get theirs when the session registers them, the REST transport takes one
    for each `poll()` table's row, and each static verb (`fetch()`, `history()`) takes one as
    its call id, which its log lines and thread names carry (`call 7`), and a background
    read's row as its `StreamId`. Thread-safe; takes only a leaf lock of its own, so it may be
    called with or without `registry_lock` held.

    Returns:
        The id, numbered from 1 within the process.
    """
    with _stream_id_lock:
        return next(_stream_numbers)
