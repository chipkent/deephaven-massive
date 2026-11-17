"""Tests for the memory cap of a connection's waiting rows and its estimate of a frame's rows."""

from __future__ import annotations

import sys

from deephaven_massive._tables.row_memory import HELD_BYTES_LIMIT, frame_bytes


def test_the_limit_is_one_gibibyte() -> None:
    assert HELD_BYTES_LIMIT == 1024**3


def test_a_frame_counts_its_list_and_its_first_row_once_per_row() -> None:
    first = ("AAPL", 101.25, 100, [12, 37])
    rows = [first, ("MSFT", 1.5, 7, [])]
    row = sys.getsizeof(first) + sum(sys.getsizeof(value) for value in first)
    assert frame_bytes(rows) == sys.getsizeof(rows) + 2 * row


def test_a_frame_of_more_rows_counts_more() -> None:
    one = [("AAPL", 1.0)]
    assert frame_bytes(one * 10) > frame_bytes(one)
