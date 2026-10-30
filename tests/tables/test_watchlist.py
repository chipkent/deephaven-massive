"""Tests for the check of a symbol table's rows and the watch that follows it."""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING, cast

import pytest
from deephaven import dtypes, input_table, new_table, time_table
from deephaven.column import int_col, string_col

import deephaven_massive as dm
from deephaven_massive._enums import AssetClass, ClassFromPrefix, OnError
from deephaven_massive._namespaces import crypto, forex, partners
from deephaven_massive._tables.liveness import new_scope
from deephaven_massive._tables.watchlist import (
    SymbolRowRule,
    SymbolRows,
    SymbolWatch,
    _SymbolCounter,
    invalid_row_message,
    read_symbols,
    skipped_rows_text,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from deephaven.table import Table
    from deephaven.table_listener import TableUpdate

    from deephaven_massive._datasets.catalog import DatasetSpec

Change = tuple[frozenset[str], frozenset[str]]


def values(table: Table, column: str = "Symbol") -> list[object]:
    return [row[0] for row in table.iter_tuple(cols=column)]


def wait_for(condition: Callable[[], bool]) -> None:
    # Bounded wait for an update-graph cycle to apply a change.
    pause = threading.Event()  # never set; only paces the loop
    for _ in range(300):
        if condition():
            return
        pause.wait(0.02)
    assert condition()


# --- read_symbols and the message ------------------------------------------------------------


FOREX = forex.QUOTES
CRYPTO = crypto.QUOTES
WATCHLIST_LOG = "deephaven_massive._tables.watchlist"


def rule(spec: DatasetSpec = FOREX, mode: OnError = OnError.RAISE) -> SymbolRowRule:
    return SymbolRowRule.of(spec, mode)


def spanning(mode: OnError = OnError.RAISE) -> SymbolRowRule:
    # A dataset whose symbols span classes: each symbol's prefix selects its class.
    return SymbolRowRule("snapshots", ClassFromPrefix.ANY, as_written=False, noncanonical=mode)


def test_read_symbols_returns_the_rows_sorted_without_nulls(dh_exec_ctx: object) -> None:
    table = new_table([string_col("Symbol", ["C:GBPUSD", None, "C:EURUSD", "C:GBPUSD"])])
    assert read_symbols(rule(), table) == SymbolRows(("C:EURUSD", "C:GBPUSD"))
    assert read_symbols(rule(), new_table([string_col("Symbol", [None])])) == SymbolRows(())


def test_read_symbols_of_a_dataset_kept_as_written(dh_exec_ctx: object) -> None:
    table = new_table([string_col("Symbol", ["ERIC B", "7203"])])
    events = rule(partners.TMX_CORPORATE_EVENTS)
    assert read_symbols(events, table).symbols == ("7203", "ERIC B")
    with pytest.raises(ValueError, match="is not a ticker as Massive writes it"):
        read_symbols(events, new_table([string_col("Symbol", [" x"])]))


@pytest.mark.parametrize("mode", list(OnError))
def test_read_symbols_refuses_a_failed_table(dh_exec_ctx: object, mode: OnError) -> None:
    source = input_table({"Symbol": dtypes.string})
    scope = new_scope()
    with scope.open():
        watch = dm.canonicalize_symbols(source, AssetClass.FOREX)
    source.add(new_table([string_col("Symbol", ["no pe!"])]))
    wait_for(lambda: bool(watch.j_table.isFailed()))
    with pytest.raises(
        ValueError,
        match=r"^the symbol table passed to forex\.quotes failed upstream, so the result can no "
        r"longer follow it: it is failed\. Fix that table and call forex\.quotes again",
    ):
        read_symbols(rule(mode=mode), watch)
    scope.release()


def test_read_symbols_refuses_a_row_that_is_not_canonical(dh_exec_ctx: object) -> None:
    table = new_table([string_col("Symbol", ["C:EURUSD", "EUR/USD"])])
    with pytest.raises(
        ValueError,
        match=r"^forex\.quotes: a symbol table holds 'EUR/USD' \('EUR/USD' is not Massive's "
        r"ticker; use 'C:EURUSD' .*\)\. Fix the row, or pass the table through "
        r"dm\.canonicalize_symbols\(table, dm\.AssetClass\.FOREX, on_error=dm\.OnError\.LOG\), "
        r"which converts what it can and nulls the rest$",
    ):
        read_symbols(rule(), table)


@pytest.mark.parametrize(("mode", "level"), [(OnError.LOG, "WARNING"), (OnError.IGNORE, "DEBUG")])
def test_read_symbols_skips_a_row_that_is_not_canonical(
    dh_exec_ctx: object, *, caplog: pytest.LogCaptureFixture, mode: OnError, level: str
) -> None:
    # A value of its own per mode: each value is logged once per dataset and process.
    bad = f"EUR/USD {mode.value}"
    table = new_table([string_col("Symbol", ["C:EURUSD", bad, bad, None])])
    with caplog.at_level("DEBUG", logger=WATCHLIST_LOG):
        assert read_symbols(rule(mode=mode), table) == SymbolRows(("C:EURUSD",), (bad,))
        read_symbols(rule(mode=mode), table)  # a second read logs nothing more
    records = [r for r in caplog.records if bad in r.getMessage()]
    assert [r.levelname for r in records] == [level]
    assert records[0].getMessage().startswith(f"forex.quotes: a symbol table holds {bad!r}")
    assert records[0].getMessage().endswith("; the row is skipped until it is fixed or deleted")


def test_the_skipped_rows_text_counts_and_lists_the_first_five() -> None:
    assert skipped_rows_text(["aapl"]) == (
        "skipping 1 symbol-table value that is not Massive's ticker: 'aapl'; fix or delete that row"
    )
    assert skipped_rows_text(["g", "f", "e", "d", "c", "b", "a"]) == (
        "skipping 7 symbol-table values that are not Massive's tickers: 'a', 'b', 'c', 'd', "
        "'e', and 2 more; fix or delete those rows"
    )
    assert skipped_rows_text(["b", "a"]) == (
        "skipping 2 symbol-table values that are not Massive's tickers: 'a', 'b'; fix or delete "
        "those rows"
    )


def test_the_message_of_a_dataset_that_spans_classes_names_no_class() -> None:
    message = invalid_row_message(ClassFromPrefix.ANY, "aapl")
    assert "tickers are case-sensitive" in message
    assert message.endswith(
        "Fix the row, or convert each row with dm.canonical_symbol(text, asset_class) and the "
        "row's own class, since this dataset's rows may mix classes"
    )


# --- SymbolWatch ---------------------------------------------------------------------------------


Watching = tuple[SymbolWatch, list[Change], list[str], list[tuple[str, ...]]]


def watching(table: Table, mode: OnError = OnError.RAISE) -> Watching:
    seen: list[Change] = []
    refused: list[str] = []
    skipped: list[tuple[str, ...]] = []
    lock = threading.Lock()

    def record(added: frozenset[str], removed: frozenset[str]) -> None:
        with lock:
            seen.append((added, removed))

    def refuse(message: str) -> None:
        with lock:
            refused.append(message)

    def skip(values: tuple[str, ...]) -> None:
        with lock:
            skipped.append(values)

    watch = SymbolWatch(rule(mode=mode), table, record, on_invalid=refuse, on_skipped=skip)
    return watch, seen, refused, skipped


def test_a_watch_reports_changes_of_the_symbol_set(dh_exec_ctx: object) -> None:
    watchlist = input_table({"Key": dtypes.int32, "Symbol": dtypes.string}, key_cols=["Key"])
    watchlist.add(
        new_table([int_col("Key", [1, 2, 3]), string_col("Symbol", ["C:EURUSD", "C:EURUSD", None])])
    )
    watch, seen, refused, skipped = watching(watchlist)
    watch.start()  # the replay: two rows of one pair count once; the null row is ignored
    assert seen == [(frozenset({"C:EURUSD"}), frozenset())]
    watchlist.delete(new_table([int_col("Key", [1])]))  # one of its two rows: still held
    watchlist.add(new_table([int_col("Key", [3]), string_col("Symbol", ["C:GBPUSD"])]))  # modify
    wait_for(lambda: len(seen) == 2)
    assert seen[1] == (frozenset({"C:GBPUSD"}), frozenset())
    watchlist.delete(new_table([int_col("Key", [2])]))
    wait_for(lambda: len(seen) == 3)
    assert seen[2] == (frozenset(), frozenset({"C:EURUSD"}))
    assert refused == []
    assert skipped == []


def test_a_watch_refuses_a_late_row_that_is_not_canonical(dh_exec_ctx: object) -> None:
    watchlist = input_table({"Key": dtypes.int32, "Symbol": dtypes.string}, key_cols=["Key"])
    watch, seen, refused, skipped = watching(watchlist)
    watch.start()
    watchlist.add(
        new_table([int_col("Key", [1, 2]), string_col("Symbol", ["EUR/USD", "C:GBPUSD"])])
    )
    wait_for(lambda: len(seen) == 1 and len(refused) == 1)
    assert seen == [(frozenset({"C:GBPUSD"}), frozenset())]  # the bad row is never subscribed
    assert refused == [f"forex.quotes: {invalid_row_message(AssetClass.FOREX, 'EUR/USD')}"]
    watchlist.delete(new_table([int_col("Key", [1])]))  # removing it changes nothing
    watchlist.add(new_table([int_col("Key", [3]), string_col("Symbol", ["C:USDJPY"])]))
    wait_for(lambda: len(seen) == 2)
    assert seen[1] == (frozenset({"C:USDJPY"}), frozenset())
    assert len(refused) == 1
    assert skipped == []


def test_a_watch_skips_a_late_row_that_is_not_canonical(dh_exec_ctx: object) -> None:
    watchlist = input_table({"Key": dtypes.int32, "Symbol": dtypes.string}, key_cols=["Key"])
    watchlist.add(new_table([int_col("Key", [1]), string_col("Symbol", ["eur/usd"])]))
    watch, seen, refused, skipped = watching(watchlist, OnError.LOG)
    watch.start()  # the replay reports the skipped row
    assert seen == []
    assert skipped == [("eur/usd",)]
    watchlist.add(
        new_table([int_col("Key", [2, 3]), string_col("Symbol", ["EUR/USD", "C:GBPUSD"])])
    )
    wait_for(lambda: len(seen) == 1 and len(skipped) == 2)
    assert seen == [(frozenset({"C:GBPUSD"}), frozenset())]  # bad rows are never subscribed
    assert skipped[1] == ("EUR/USD", "eur/usd")
    watchlist.add(new_table([int_col("Key", [2]), string_col("Symbol", ["C:EURUSD"])]))  # fixed
    wait_for(lambda: len(seen) == 2 and len(skipped) == 3)
    assert seen[1] == (frozenset({"C:EURUSD"}), frozenset())
    assert skipped[2] == ("eur/usd",)
    watchlist.delete(new_table([int_col("Key", [1])]))  # the last bad row goes
    wait_for(lambda: len(skipped) == 4)
    assert skipped[3] == ()
    assert refused == []


def test_an_empty_table_reports_nothing(dh_exec_ctx: object) -> None:
    watch, seen, _, skipped = watching(input_table({"Symbol": dtypes.string}))
    watch.start()
    assert seen == []
    assert skipped == []


def test_a_failing_watch_is_logged_and_refused(caplog: pytest.LogCaptureFixture) -> None:
    refused: list[str] = []
    counter = _SymbolCounter(
        spanning(), lambda *_: None, on_invalid=refused.append, on_skipped=lambda _: None
    )
    counter.on_error(RuntimeError("table broke"))
    expected = (
        "the symbol table passed to snapshots failed upstream, so the result can no longer "
        "follow it: table broke. Fix that table and call snapshots again; a table made by "
        "dm.canonicalize_symbols fails on a row it cannot convert unless on_error=dm.OnError.LOG"
    )
    assert [(r.levelname, r.getMessage()) for r in caplog.records] == [("ERROR", expected)]
    assert refused == [expected]


def test_a_watch_over_a_failing_canonicalized_table_refuses_it(dh_exec_ctx: object) -> None:
    source = input_table({"Symbol": dtypes.string})
    source.add(new_table([string_col("Symbol", ["EUR/USD"])]))
    scope = new_scope()
    with scope.open():
        watch_table = dm.canonicalize_symbols(source, AssetClass.FOREX)
        watch, seen, refused, _ = watching(watch_table, OnError.IGNORE)
    watch.start()
    assert seen == [(frozenset({"C:EURUSD"}), frozenset())]
    source.add(new_table([string_col("Symbol", ["no pe!"])]))
    wait_for(lambda: len(refused) == 1)
    assert refused[0].startswith("the symbol table passed to forex.quotes failed upstream")
    scope.release()


def test_a_watch_lives_as_long_as_its_keeper(dh_exec_ctx: object) -> None:
    watchlist = input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    keeper_scope = new_scope()
    with keeper_scope.open():
        keeper = time_table("PT1s")
    watch_scope = new_scope()
    with watch_scope.open():
        watch, seen, _, _ = watching(watchlist)
        watch.keep_alive_with(keeper)
    watch.start()
    watch_scope.release()  # the keeper still holds the view and the listener
    watchlist.add(new_table([string_col("Symbol", ["C:EURUSD"])]))
    wait_for(lambda: len(seen) == 1)
    watch.stop()  # no more reports; a second stop does nothing
    watch.stop()
    keeper_scope.release()


class _Update:
    # The parts of a `TableUpdate` the counter reads, for calls from a Python thread: Deephaven
    # delivers real updates on its own threads, which line coverage before 3.14 cannot see.
    def __init__(self, **columns: list[str]) -> None:
        self._columns = columns

    def added(self, cols: list[str]) -> dict[str, list[str]]:
        return {cols[0]: self._columns.get("added", [])}

    def modified(self, cols: list[str]) -> dict[str, list[str]]:
        return {cols[0]: self._columns.get("modified", [])}

    def modified_prev(self, cols: list[str]) -> dict[str, list[str]]:
        return {cols[0]: self._columns.get("modified_prev", [])}

    def removed(self, cols: list[str]) -> dict[str, list[str]]:
        return {cols[0]: self._columns.get("removed", [])}


_UPDATES = [
    _Update(added=["A", "A", "B", "b"]),
    _Update(removed=["A", "b"]),  # one of A's two rows remains; the bad row leaves
    _Update(modified=["C"], modified_prev=["A"]),  # A's last row now holds C
    _Update(modified=["x:btc"], modified_prev=["C"]),  # C's row now holds a bad symbol
]


def test_the_counter_reports_set_changes_of_updates() -> None:
    seen: list[Change] = []
    refused: list[str] = []
    skipped: list[tuple[str, ...]] = []
    counter = _SymbolCounter(
        spanning(),
        lambda a, r: seen.append((a, r)),
        on_invalid=refused.append,
        on_skipped=skipped.append,
    )
    for update in _UPDATES:
        counter.on_update(cast("TableUpdate", update), is_replay=False)
    assert seen == [
        (frozenset({"A", "B"}), frozenset()),
        (frozenset({"C"}), frozenset({"A"})),
        (frozenset(), frozenset({"C"})),
    ]
    assert refused == [
        f"snapshots: {invalid_row_message(ClassFromPrefix.ANY, 'b')}",
        f"snapshots: {invalid_row_message(ClassFromPrefix.ANY, 'x:btc')}",
    ]
    assert skipped == []


def test_the_counter_reports_skipped_values_of_updates() -> None:
    seen: list[Change] = []
    refused: list[str] = []
    skipped: list[tuple[str, ...]] = []
    counter = _SymbolCounter(
        spanning(OnError.IGNORE),
        lambda a, r: seen.append((a, r)),
        on_invalid=refused.append,
        on_skipped=skipped.append,
    )
    for update in _UPDATES:
        counter.on_update(cast("TableUpdate", update), is_replay=False)
    assert seen == [
        (frozenset({"A", "B"}), frozenset()),
        (frozenset({"C"}), frozenset({"A"})),
        (frozenset(), frozenset({"C"})),
    ]
    assert skipped == [("b",), (), ("x:btc",)]  # only when the set of skipped values changes
    assert refused == []
