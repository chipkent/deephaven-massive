"""Tests for judging a static read's unreadable rows across all its pages or files."""

import threading

import pyarrow as pa
import pytest

from deephaven_massive._errors import RowDataError
from deephaven_massive._massive import RowFault
from deephaven_massive._tables.read_tally import ReadTally
from deephaven_massive._tables.withheld import Withheld, WithheldGroup

NO_ROWS = pa.table({"x": pa.array([], pa.int64())})


def withheld(rows: int, reason: str) -> Withheld:
    # A payload with `rows` rows withheld for `reason`, and none published.
    if not rows:
        return Withheld(NO_ROWS)
    group = WithheldGroup(RowFault.UNCONVERTIBLE_VALUES, "AAPL", "AAPL", "Price", rows, reason)
    return Withheld(NO_ROWS, (group,), reason)


def test_a_read_with_one_row_read_whole_never_fails() -> None:
    tally = ReadTally()
    tally.add(3, withheld(3, "first"))
    tally.add(1, withheld(0, ""))
    tally.check("stocks.trades", source="GET /v3/trades/{stockTicker}")


def test_a_read_with_no_rows_of_its_symbols_and_window_never_fails() -> None:
    tally = ReadTally()
    tally.add(0, withheld(0, ""))
    tally.check("stocks.trades", source="GET /v3/trades/{stockTicker}")
    ReadTally().check("stocks.trades", source="GET /v3/trades/{stockTicker}")


def test_a_read_whose_rows_are_all_withheld_fails_with_its_first_reason() -> None:
    tally = ReadTally()
    tally.add(0, withheld(0, ""))
    tally.add(2, withheld(2, "first"))
    tally.add(1, withheld(1, "second"))
    with pytest.raises(
        RowDataError,
        match=r"^stocks\.trades: the library could read none of the 3 rows of the read's "
        r"symbols and window from 2 flat files whole, so Massive's format may have changed; "
        r"the first: first",
    ):
        tally.check("stocks.trades", source="2 flat files")


def test_threads_adding_at_once_lose_no_count() -> None:
    tally = ReadTally()
    start = threading.Barrier(4)

    def add() -> None:
        start.wait()
        for _ in range(1000):
            tally.add(1, withheld(1, "x"))

    threads = [threading.Thread(target=add) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    with pytest.raises(RowDataError, match="none of the 4000 rows"):
        tally.check("stocks.trades", source="2 flat files")
