"""Tests for judging a read's unreadable rows across all its pages, files, and parts."""

import threading

import pyarrow as pa
import pytest
from deephaven import arrow as dh_arrow

import deephaven_massive as dm
from deephaven_massive._errors import ErrorSource, RowDataError
from deephaven_massive._massive import RowFault
from deephaven_massive._tables.read_tally import ReadTally
from deephaven_massive._tables.transports import SymbolFailure, SymbolOutcome, SymbolRows
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
    tally.add(3, withheld(3, "first"), source="GET /v3/trades/AAPL")
    tally.add(1, withheld(0, ""), source="GET /v3/trades/AAPL")
    assert tally.rows == 4
    tally.check("stocks.trades")


def test_a_read_with_no_rows_of_its_symbols_and_window_never_fails() -> None:
    tally = ReadTally()
    tally.add(0, withheld(0, ""), source="GET /v3/trades/AAPL")
    tally.check("stocks.trades")
    ReadTally().check("stocks.trades")


def test_a_read_whose_rows_are_all_withheld_fails_with_its_first_reason() -> None:
    tally = ReadTally()
    # Each part names what it read; a part with no rows of the read is not named.
    tally.add(0, withheld(0, ""), source="GET /v3/trades/AAPL")
    tally.add(2, withheld(2, "first"), source="the flat files of us_stocks_sip/trades_v1")
    tally.add(1, withheld(1, "second"), source="GET /v3/trades/AAPL")
    tally.add(1, withheld(1, "third"), source="the flat files of us_stocks_sip/trades_v1")
    with pytest.raises(RowDataError) as info:
        tally.check("stocks.trades")
    assert str(info.value) == (
        "stocks.trades: the library could read none of the 4 rows of the read's symbols and "
        "window from the flat files of us_stocks_sip/trades_v1, GET /v3/trades/AAPL "
        "whole, so it withheld them all. First. m.problems() counts the withheld rows by cause "
        "and symbol."
    )
    assert (info.value.source, info.value.is_bug) == (ErrorSource.MASSIVE, False)


def test_threads_adding_at_once_lose_no_count() -> None:
    tally = ReadTally()
    start = threading.Barrier(4)

    def add() -> None:
        start.wait()
        for _ in range(1000):
            tally.add(1, withheld(1, "x"), source="a file")

    threads = [threading.Thread(target=add) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    with pytest.raises(RowDataError, match="none of the 4000 rows"):
        tally.check("stocks.trades")


def test_judge_fails_only_the_symbols_a_wholly_withheld_read_gave_rows() -> None:
    rows = dh_arrow.to_table(pa.table({"x": pa.array([], pa.int64())}))
    own = dm.MassiveError("IBM's own failure")
    outcomes: dict[str, SymbolOutcome] = {
        "AAPL": SymbolRows(rows),
        "MSFT": SymbolRows(rows, no_data=True),  # Massive has no data for it: it stands
        "IBM": SymbolFailure(own),  # failed on its own: it keeps its failure
    }
    whole = ReadTally()
    whole.add(1, withheld(0, ""), source="GET /v3/trades/AAPL")
    assert whole.judge("stocks.trades", outcomes) == outcomes
    lost = ReadTally()
    lost.add(1, withheld(1, "first"), source="GET /v3/trades/AAPL")
    judged = lost.judge("stocks.trades", outcomes)
    assert list(judged) == ["AAPL", "MSFT", "IBM"]
    failed = judged["AAPL"]
    assert isinstance(failed, SymbolFailure)
    assert isinstance(failed.error, RowDataError)
    assert "none of the 1 rows" in str(failed.error)
    assert judged["MSFT"] is outcomes["MSFT"]
    assert judged["IBM"] is outcomes["IBM"]
