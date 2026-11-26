"""Tests for judging a read's unreadable rows across all its pages, files, and parts."""

import logging
import threading
from collections.abc import Callable

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
        "stocks.trades: Massive sent 4 rows of the read's symbols and window from the flat files "
        "of us_stocks_sip/trades_v1, GET /v3/trades/AAPL, and the library could read none of them "
        "whole, so it withheld them all. First. m.problems() counts the withheld rows by cause and "
        "symbol."
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
    with pytest.raises(RowDataError, match="Massive sent 4,000 rows "):
        tally.check("stocks.trades")


NONE = dh_arrow.to_table(NO_ROWS)
"""A symbol's table with no rows."""

ONE = dh_arrow.to_table(pa.table({"x": pa.array([1], pa.int64())}))
"""A symbol's table with one row."""


def group(symbol: str | None, rows: int, reason: str) -> WithheldGroup:
    # `rows` rows of `symbol` (None: not read) withheld for `reason`.
    return WithheldGroup(RowFault.UNCONVERTIBLE_VALUES, symbol, symbol, "Price", rows, reason)


def owners_in(*own: str) -> Callable[[str | None], tuple[str, ...]]:
    # The asked symbols of a request of `own` a withheld row could be (`symbol_reads._owners`).
    return lambda symbol: own if symbol is None else ((symbol,) if symbol in own else ())


def test_judge_fails_a_symbol_only_for_its_own_withheld_rows(
    caplog: pytest.LogCaptureFixture,
) -> None:
    own = dm.MassiveError("IBM's own failure")
    outcomes: dict[str, SymbolOutcome] = {
        "AAPL": SymbolRows(NONE),  # no row, and its own rows withheld: it fails
        "MSFT": SymbolRows(NONE),  # no row, and none withheld: Massive had none for it
        "NVDA": SymbolRows(ONE),  # a row read whole beside its withheld one: it stands
        "IBM": SymbolFailure(own),  # failed on its own: it keeps its failure
    }
    tally = ReadTally()
    rows = Withheld(NO_ROWS, (group("AAPL", 2, "First"), group("NVDA", 1, "x")), "First")
    tally.add_request(
        kept=1, withheld=rows, owners=owners_in("AAPL", "MSFT", "NVDA"), source="GET /v3/x"
    )
    with caplog.at_level(logging.DEBUG, logger="deephaven_massive._tables.read_tally"):
        judged = tally.judge("stocks.trades", outcomes)
    assert list(judged) == ["AAPL", "MSFT", "NVDA", "IBM"]
    failed = judged["AAPL"]
    assert isinstance(failed, SymbolFailure)
    assert isinstance(failed.error, RowDataError)
    assert str(failed.error) == (
        "stocks.trades: Massive sent 2 rows for AAPL from GET /v3/x, and the library could read "
        "none of them whole, so it withheld them all. First. m.problems() counts the withheld rows "
        "by cause and symbol."
    )
    named = str(failed.error).removeprefix("stocks.trades: ")  # the log names the dataset once
    assert caplog.messages[-1] == f"stocks.trades by symbol fails AAPL: {named}"
    assert [judged[s] is outcomes[s] for s in ("MSFT", "NVDA", "IBM")] == [True] * 3


def test_a_withheld_row_of_another_symbol_fails_no_one() -> None:
    tally = ReadTally()
    rows = Withheld(NO_ROWS, (group("IBM", 1, "x"),), "x")
    tally.add_request(kept=0, withheld=rows, owners=owners_in("AAPL"), source="GET /v3/x")
    outcomes: dict[str, SymbolOutcome] = {"AAPL": SymbolRows(NONE)}
    assert tally.judge("stocks.trades", outcomes) == outcomes


def test_a_row_of_no_known_symbol_fails_its_request_s_symbols_only_when_it_kept_none() -> None:
    outcomes: dict[str, SymbolOutcome] = {
        "AAPL": SymbolRows(NONE),
        "MSFT": SymbolRows(NONE),
        "NVDA": SymbolRows(NONE),  # another request's symbol
    }
    unread = Withheld(NO_ROWS, (group(None, 2, "Unread"),), "Unread")
    nothing = ReadTally()
    nothing.add_request(
        kept=0, withheld=unread, owners=owners_in("AAPL", "MSFT"), source="GET /v3/x"
    )
    nothing.add_request(
        kept=0, withheld=Withheld(NO_ROWS), owners=owners_in("NVDA"), source="GET /v3/y"
    )
    judged = nothing.judge("stocks.trades", outcomes)
    for symbol in ("AAPL", "MSFT"):
        failed = judged[symbol]
        assert isinstance(failed, SymbolFailure)
        assert str(failed.error) == (
            "stocks.trades: Massive sent 2 rows for AAPL and MSFT from GET /v3/x, and the library "
            "could read none of them whole, so it withheld them all. Unread. m.problems() counts "
            "the withheld rows by cause and symbol."
        )
    assert judged["NVDA"] is outcomes["NVDA"]
    # The same rows beside one the request kept, of a symbol that now has it: no one fails.
    some = ReadTally()
    some.add_request(kept=1, withheld=unread, owners=owners_in("AAPL", "MSFT"), source="GET /x")
    both: dict[str, SymbolOutcome] = {"AAPL": SymbolRows(ONE), "MSFT": SymbolRows(NONE)}
    assert some.judge("stocks.trades", both) == both


def test_a_scan_row_of_no_known_symbol_could_be_any_symbol_s() -> None:
    tally = ReadTally()
    tally.add(1, Withheld(NO_ROWS, (group(None, 1, "Unread"),), "Unread"), source="the files")
    outcomes: dict[str, SymbolOutcome] = {"AAPL": SymbolRows(NONE), "MSFT": SymbolRows(NONE)}
    judged = tally.judge("stocks.trades", outcomes)
    failed = judged["MSFT"]
    assert isinstance(failed, SymbolFailure)
    assert str(failed.error) == (
        "stocks.trades: Massive sent 1 row for AAPL and MSFT from the files, and the library could "
        "not read it whole, so it withheld it. Unread. m.problems() counts the withheld rows by "
        "cause and symbol."
    )
    # A scan that kept a row fails no symbol for it.
    tally.add(1, withheld(0, ""), source="the files")
    assert tally.judge("stocks.trades", outcomes) == outcomes


def test_a_value_s_failure_names_the_value() -> None:
    tally = ReadTally()
    tally.add(1, withheld(1, "First"), source="GET /v3/snapshot/options/AAPL")
    failure = tally.failure("options.chain", whose="for AAPL")
    assert failure is not None
    assert str(failure) == (
        "options.chain: Massive sent 1 row for AAPL from GET /v3/snapshot/options/AAPL, and the "
        "library could not read it whole, so it withheld it. First. m.problems() counts the "
        "withheld rows by cause and symbol."
    )
