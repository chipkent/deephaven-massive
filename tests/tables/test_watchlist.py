"""Tests for the check of a symbol table's rows and the watch that follows it."""

from __future__ import annotations

import importlib
import logging
import threading
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

import pytest
from deephaven import dtypes, empty_table, new_table, time_table, update_graph
from deephaven.column import datetime_col, int_col, string_col
from deephaven.stream import blink_to_append_only

import deephaven_massive as dm
from deephaven_massive._datasets.request import ValueTable
from deephaven_massive._datasets.row_times import ISSUES_URL
from deephaven_massive._enums import AssetClass, ClassFromPrefix, OnError
from deephaven_massive._namespaces import crypto, forex, futures, options, partners
from deephaven_massive._tables.execution_context import update_cycle
from deephaven_massive._tables.liveness import new_scope, release_scope
from deephaven_massive._tables.publisher import BlinkPublisher, blink_publisher
from deephaven_massive._tables.watchlist import (
    CURRENT_STEP,
    SymbolRowRule,
    SymbolRows,
    SymbolWatch,
    _SymbolCounter,
    invalid_row_message,
    latest_rows_since_added,
    read_symbols,
    skipped_rows_text,
    watched_rows,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

    from deephaven.execution_context import ExecutionContext
    from deephaven.liveness_scope import LivenessScope
    from deephaven.table import Table
    from deephaven.table_factory import InputTable
    from deephaven.table_listener import TableUpdate

    from deephaven_massive._datasets.catalog import DatasetSpec

Change = tuple[frozenset[str], frozenset[str]]
PRICES = {"Symbol": dtypes.string, "Price": dtypes.int32}
STAMPED = {**PRICES, "TimestampInsert": dtypes.Instant}
EXCHANGED = {**STAMPED, "Exchange": dtypes.int32}
_JPY = cast("Any", importlib.import_module("jpy"))
_JAVA_BOOLEAN = _JPY.get_type("java.lang.Boolean")


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
    assert read_symbols(rule(), table, stream_id=7) == SymbolRows(("C:EURUSD", "C:GBPUSD"))
    assert read_symbols(
        rule(), new_table([string_col("Symbol", [None])]), stream_id=None
    ) == SymbolRows(())


def test_read_symbols_of_a_dataset_kept_as_written(dh_exec_ctx: object) -> None:
    table = new_table([string_col("Symbol", ["ERIC B", "7203"])])
    events = rule(partners.TMX_CORPORATE_EVENTS)
    assert read_symbols(events, table, stream_id=7).symbols == ("7203", "ERIC B")
    with pytest.raises(ValueError, match="is not a ticker as Massive writes it"):
        read_symbols(events, new_table([string_col("Symbol", [" x"])]), stream_id=None)


@pytest.mark.parametrize("mode", list(OnError))
def test_read_symbols_refuses_a_failed_table(
    dh_exec_ctx: object, mode: OnError, dh_input_table: Callable[..., InputTable]
) -> None:
    source = dh_input_table({"Symbol": dtypes.string})
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
        read_symbols(rule(mode=mode), watch, stream_id=7)
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
        read_symbols(rule(), table, stream_id=7)


def test_a_skipped_row_read_before_any_stream_names_the_call(
    dh_exec_ctx: object, caplog: pytest.LogCaptureFixture
) -> None:
    bad = "EUR/USD call"  # a value of its own: each value is logged once per dataset and process
    table = new_table([string_col("Symbol", [bad])])
    with caplog.at_level("WARNING", logger=WATCHLIST_LOG):
        read_symbols(rule(mode=OnError.LOG), table, stream_id=None)
    [record] = [r.getMessage() for r in caplog.records if bad in r.getMessage()]
    assert record.startswith(f"forex.quotes a call's read: a symbol table holds {bad!r}")


@pytest.mark.parametrize(("mode", "level"), [(OnError.LOG, "WARNING"), (OnError.IGNORE, "DEBUG")])
def test_read_symbols_skips_a_row_that_is_not_canonical(
    dh_exec_ctx: object, *, caplog: pytest.LogCaptureFixture, mode: OnError, level: str
) -> None:
    # A value of its own per mode: each value is logged once per dataset and process.
    bad = f"EUR/USD {mode.value}"
    table = new_table([string_col("Symbol", ["C:EURUSD", bad, bad, None])])
    with caplog.at_level("DEBUG", logger=WATCHLIST_LOG):
        assert read_symbols(rule(mode=mode), table, stream_id=7) == SymbolRows(
            ("C:EURUSD",), (bad,)
        )
        read_symbols(rule(mode=mode), table, stream_id=7)  # a second read logs nothing more
    records = [r for r in caplog.records if bad in r.getMessage()]
    assert [r.levelname for r in records] == [level]
    assert (
        records[0].getMessage().startswith(f"forex.quotes stream 7: a symbol table holds {bad!r}")
    )
    assert records[0].getMessage().endswith("; the row is skipped until it is fixed or deleted")


def test_the_skipped_rows_text_counts_and_lists_the_first_five() -> None:
    # The value is not called wrong: Massive may write a form the library does not know yet.
    assert skipped_rows_text(["aapl"]) == (
        "skipping 1 symbol-table value this dataset does not take: 'aapl'; fix or delete that "
        f"row, or report it at {ISSUES_URL} if Massive itself writes it"
    )
    assert skipped_rows_text(["g", "f", "e", "d", "c", "b", "a"]) == (
        "skipping 7 symbol-table values this dataset does not take: 'a', 'b', 'c', 'd', 'e', "
        f"and 2 more; fix or delete those rows, or report them at {ISSUES_URL} if Massive "
        "itself writes them"
    )
    assert skipped_rows_text(["b", "a"]).startswith(
        "skipping 2 symbol-table values this dataset does not take: 'a', 'b'; fix or delete "
    )


def test_the_message_of_a_dataset_that_spans_classes_names_no_class() -> None:
    message = invalid_row_message(ClassFromPrefix.ANY, "aapl")
    assert "tickers are case-sensitive" in message
    assert message.endswith(
        "Fix the row, or convert each row with dm.canonical_symbol(text, asset_class) and the "
        "row's own class, since this dataset's rows may mix classes. If Massive itself writes "
        f"'aapl', the library does not know its form yet: report it at {ISSUES_URL}"
    )


# --- SymbolWatch ---------------------------------------------------------------------------------


Watching = tuple[SymbolWatch, list[Change], list[str], list[tuple[str, ...]]]


def watching(
    table: Table, mode: OnError = OnError.RAISE, row_rule: SymbolRowRule | None = None
) -> Watching:
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

    watch = SymbolWatch(
        rule(mode=mode) if row_rule is None else row_rule,
        table,
        record,
        stream_id=7,
        on_invalid=refuse,
        on_skipped=skip,
    )
    return watch, seen, refused, skipped


def test_a_watch_reports_changes_of_the_symbol_set(
    dh_exec_ctx: object, dh_input_table: Callable[..., InputTable]
) -> None:
    watchlist = dh_input_table({"Key": dtypes.int32, "Symbol": dtypes.string}, key_cols=["Key"])
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


def test_a_watch_refuses_a_late_row_that_is_not_canonical(
    dh_exec_ctx: object, dh_input_table: Callable[..., InputTable]
) -> None:
    watchlist = dh_input_table({"Key": dtypes.int32, "Symbol": dtypes.string}, key_cols=["Key"])
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


def test_a_watch_skips_a_late_row_that_is_not_canonical(
    dh_exec_ctx: object, dh_input_table: Callable[..., InputTable]
) -> None:
    watchlist = dh_input_table({"Key": dtypes.int32, "Symbol": dtypes.string}, key_cols=["Key"])
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


def underlyings(mode: OnError = OnError.RAISE) -> SymbolRowRule:
    # The rule of `m.options.chain(underlying=table)`: stock and index tickers in `Symbol`.
    table = new_table([string_col("Symbol", ["AAPL"])])
    values = ValueTable("underlying", "underlyingAsset", "Symbol", underlying=True, table=table)
    return SymbolRowRule.for_values(options.CHAIN, values, mode)


def product_codes(mode: OnError = OnError.RAISE) -> SymbolRowRule:
    # The rule of a futures `product_code=table`: any code as written, in `ProductCode`.
    table = new_table([string_col("ProductCode", ["ES"])])
    values = ValueTable(
        "product_code", "product_code", "ProductCode", underlying=False, table=table
    )
    return SymbolRowRule.for_values(futures.PRODUCTS, values, mode)


def test_a_table_of_underlyings_takes_only_stock_and_index_tickers(dh_exec_ctx: object) -> None:
    table = new_table([string_col("Symbol", ["I:SPX", "BRK.B", None, "AAPL"])])
    assert read_symbols(underlyings(), table, stream_id=7).symbols == ("AAPL", "BRK.B", "I:SPX")
    option = "O:AAPL250117C00150000"
    for refused in (option, "X:BTCUSD", "aapl"):
        assert not underlyings().accepts(refused)
    with pytest.raises(
        ValueError,
        match=r"^options\.chain: a table of underlyings holds 'O:AAPL250117C00150000', which is "
        r"not a stock or index ticker such as 'AAPL' or 'I:SPX'\. Fix the row, or convert it with "
        r"dm\.canonical_symbol\(text, asset_class\)$",
    ):
        read_symbols(underlyings(), new_table([string_col("Symbol", [option])]), stream_id=7)


def test_a_skipped_underlying_is_logged_with_its_reason(
    dh_exec_ctx: object, caplog: pytest.LogCaptureFixture
) -> None:
    bad = "X:ETHUSD"  # a value of its own: each value is logged once per dataset and process
    table = new_table([string_col("Symbol", [bad, "AAPL"])])
    with caplog.at_level("WARNING", logger=WATCHLIST_LOG):
        rows = read_symbols(underlyings(OnError.LOG), table, stream_id=None)
    assert rows == SymbolRows(("AAPL",), (bad,))
    [record] = [r.getMessage() for r in caplog.records if bad in r.getMessage()]
    assert record.startswith(f"options.chain a call's read: a table of underlyings holds {bad!r}")


def test_a_table_of_product_codes_is_read_from_its_product_code_column(
    dh_exec_ctx: object,
) -> None:
    table = new_table([string_col("ProductCode", ["NQ", "6E", None, "ES"])])
    assert read_symbols(product_codes(), table, stream_id=7).symbols == ("6E", "ES", "NQ")
    with pytest.raises(ValueError, match="is not a ticker as Massive writes it"):
        read_symbols(product_codes(), new_table([string_col("ProductCode", [" ES"])]), stream_id=7)


def test_a_watch_of_product_codes_reports_changes_of_its_codes(
    dh_exec_ctx: object, dh_input_table: Callable[..., InputTable]
) -> None:
    codes = dh_input_table({"ProductCode": dtypes.string}, key_cols=["ProductCode"])
    codes.add(new_table([string_col("ProductCode", ["ES"])]))
    watch, seen, refused, _ = watching(codes, row_rule=product_codes())
    watch.start()
    assert seen == [(frozenset({"ES"}), frozenset())]
    codes.add(new_table([string_col("ProductCode", ["NQ"])]))
    wait_for(lambda: len(seen) == 2)
    assert seen[1] == (frozenset({"NQ"}), frozenset())
    assert refused == []


def test_an_empty_table_reports_nothing(
    dh_exec_ctx: object, dh_input_table: Callable[..., InputTable]
) -> None:
    watch, seen, _, skipped = watching(dh_input_table({"Symbol": dtypes.string}))
    watch.start()
    assert seen == []
    assert skipped == []


def test_a_failing_watch_is_refused_and_logged_only_at_debug(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # The stream the refusal fails logs it at ERROR; the watch adds only a DEBUG record.
    refused: list[str] = []
    counter = _SymbolCounter(
        spanning(),
        lambda *_: None,
        stream_id=7,
        on_invalid=refused.append,
        on_skipped=lambda _: None,
    )
    with caplog.at_level(logging.DEBUG, logger=WATCHLIST_LOG):
        counter.on_error(RuntimeError("table broke"))
    expected = (
        "the symbol table passed to snapshots failed upstream, so the result can no longer "
        "follow it: table broke. Fix that table and call snapshots again; a table made by "
        "dm.canonicalize_symbols fails on a row it cannot convert unless on_error=dm.OnError.LOG"
    )
    assert [(r.levelname, r.getMessage()) for r in caplog.records] == [
        ("DEBUG", f"snapshots stream 7: {expected}")
    ]
    assert refused == [expected]


def test_a_watch_over_a_failing_canonicalized_table_refuses_it(
    dh_exec_ctx: object, dh_input_table: Callable[..., InputTable]
) -> None:
    source = dh_input_table({"Symbol": dtypes.string})
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


def test_a_watch_lives_as_long_as_its_keeper(
    dh_exec_ctx: object, dh_input_table: Callable[..., InputTable]
) -> None:
    watchlist = dh_input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
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
        stream_id=7,
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
        stream_id=7,
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


# --- watched_rows ----------------------------------------------------------------------------


def test_watched_rows_keeps_the_watched_symbols_and_follows_the_watchlist(
    dh_exec_ctx: ExecutionContext,
    dh_input_table: Callable[..., InputTable],
) -> None:
    scope = new_scope()
    with scope.open():
        prices = dh_input_table(
            {"Symbol": dtypes.string, "Price": dtypes.int32}, key_cols=["Symbol"]
        )
        prices.add(new_table([string_col("Symbol", ["A", "B", "C"]), int_col("Price", [1, 2, 3])]))
        watchlist = dh_input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
        watchlist.add(new_table([string_col("Symbol", ["A", "B"])]))
        kept = watched_rows(prices, watchlist)
    wait_for(lambda: sorted(cast("list[str]", values(kept))) == ["A", "B"])
    assert kept.column_names == ["Symbol", "Price"]  # the hidden mark is dropped
    watchlist.delete(new_table([string_col("Symbol", ["B"])]))
    wait_for(lambda: values(kept) == ["A"])
    watchlist.add(new_table([string_col("Symbol", ["C"])]))
    wait_for(lambda: sorted(cast("list[str]", values(kept))) == ["A", "C"])
    release_scope(dh_exec_ctx, scope)


def test_watched_rows_keeps_a_blink_table_blink_and_counts_a_repeated_symbol_once(
    dh_exec_ctx: ExecutionContext,
) -> None:
    pins: list[LivenessScope] = []
    scope = new_scope()
    with scope.open():
        blink, publisher = blink_publisher(
            "watched", PRICES, on_shutdown=lambda: None, release_later=pins.append
        )
        kept = watched_rows(blink, new_table([string_col("Symbol", ["A", "A"])]))
        latest = kept.last_by("Symbol")
    assert kept.is_blink  # so `last_by` keeps each symbol's row after its cycle
    publisher.add(new_table([string_col("Symbol", ["A", "B"]), int_col("Price", [1, 2])]))
    wait_for(lambda: [tuple(r) for r in latest.iter_tuple()] == [("A", 1)])
    publisher.add(new_table([string_col("Symbol", ["A"]), int_col("Price", [5])]))
    wait_for(lambda: [tuple(r) for r in latest.iter_tuple()] == [("A", 5)])
    release_scope(dh_exec_ctx, scope)


def stamped(symbol: str, price: int, *, at: int | None = None, exchange: int = 1) -> Table:
    # One row as the library stamps it: received `at` (epoch nanoseconds), by default now.
    return new_table(
        [
            string_col("Symbol", [symbol]),
            int_col("Price", [price]),
            datetime_col("TimestampInsert", [time.time_ns() if at is None else at]),
            int_col("Exchange", [exchange]),
        ]
    )


def keyed_watchlist(symbols: list[str], *, dh_input_table: Callable[..., InputTable]) -> InputTable:
    # An editable watchlist keyed by symbol.
    watchlist = dh_input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
    watchlist.add(new_table([string_col("Symbol", symbols)]))
    return watchlist


@dataclass
class Latest:
    """A stream's blink table, its publisher, and `latest_rows_since_added` over a watchlist."""

    publisher: BlinkPublisher
    kept: Table
    scope: LivenessScope
    pins: list[LivenessScope]

    def shown(
        self, dh_to_pylist: Callable[[Table], list[dict[str, object]]]
    ) -> list[tuple[str, int]]:
        return sorted(
            (cast("str", r["Symbol"]), cast("int", r["Price"])) for r in dh_to_pylist(self.kept)
        )

    def release(self, dh_exec_ctx: ExecutionContext) -> None:
        release_scope(dh_exec_ctx, self.scope)
        for pin in self.pins:
            release_scope(dh_exec_ctx, pin)


def latest_of(watchlist: Table, keys: tuple[str, ...] = ("Symbol",)) -> Latest:
    pins: list[LivenessScope] = []
    scope = new_scope()
    with scope.open():
        blink, publisher = blink_publisher(
            "latest", EXCHANGED, on_shutdown=lambda: None, release_later=pins.append
        )
        kept = latest_rows_since_added(blink, watchlist, keys)
    return Latest(publisher, kept, scope, pins)


def cycle_ended(dh_exec_ctx: ExecutionContext) -> None:
    # Taking the shared lock waits for the running update cycle to end.
    with update_graph.shared_lock(dh_exec_ctx.update_graph):
        pass


def test_latest_rows_since_added_shows_a_row_received_before_the_build(
    dh_exec_ctx: ExecutionContext,
    dh_to_pylist: Callable[[Table], list[dict[str, object]]],
    dh_input_table: Callable[..., InputTable],
) -> None:
    # A row the library received while the result was being built reaches the table after
    # it: it shows, whatever its receive time says.
    received = time.time_ns()
    watchlist = keyed_watchlist(["A"], dh_input_table=dh_input_table)
    latest = latest_of(watchlist)
    latest.publisher.add(stamped("A", 1, at=received))
    wait_for(lambda: latest.shown(dh_to_pylist) == [("A", 1)])
    assert latest.kept.column_names == list(EXCHANGED)  # both hidden steps are dropped
    latest.release(dh_exec_ctx)


def test_latest_rows_since_added_shows_a_row_that_arrives_in_the_cycle_its_symbol_joins(
    dh_exec_ctx: ExecutionContext, dh_to_pylist: Callable[[Table], list[dict[str, object]]]
) -> None:
    pins: list[LivenessScope] = []
    scope = new_scope()
    with scope.open():
        watch_blink, watch_publisher = blink_publisher(
            "watch", {"Symbol": dtypes.string}, on_shutdown=lambda: None, release_later=pins.append
        )
        latest = latest_of(blink_to_append_only(watch_blink))
    received = time.time_ns()
    with update_graph.exclusive_lock(dh_exec_ctx.update_graph):  # both land in one cycle
        watch_publisher.add(new_table([string_col("Symbol", ["C"])]))
        latest.publisher.add(stamped("C", 9, at=received))
    wait_for(lambda: latest.shown(dh_to_pylist) == [("C", 9)])
    latest.release(dh_exec_ctx)
    release_scope(dh_exec_ctx, scope)
    for pin in pins:
        release_scope(dh_exec_ctx, pin)


def test_latest_rows_since_added_reads_no_wall_clock(
    dh_exec_ctx: ExecutionContext,
    dh_to_pylist: Callable[[Table], list[dict[str, object]]],
    dh_input_table: Callable[..., InputTable],
) -> None:
    watchlist = keyed_watchlist(["A"], dh_input_table=dh_input_table)
    latest = latest_of(watchlist)
    latest.publisher.add(stamped("A", 1, at=946_684_800_000_000_000))  # 2000-01-01
    wait_for(lambda: latest.shown(dh_to_pylist) == [("A", 1)])
    latest.release(dh_exec_ctx)


def test_latest_rows_since_added_hides_a_re_added_symbols_old_row(
    dh_exec_ctx: ExecutionContext,
    dh_to_pylist: Callable[[Table], list[dict[str, object]]],
    dh_input_table: Callable[..., InputTable],
) -> None:
    watchlist = keyed_watchlist(["A", "B"], dh_input_table=dh_input_table)
    latest = latest_of(watchlist)
    latest.publisher.add(stamped("A", 1))
    latest.publisher.add(stamped("B", 2))
    wait_for(lambda: latest.shown(dh_to_pylist) == [("A", 1), ("B", 2)])
    watchlist.delete(new_table([string_col("Symbol", ["A"])]))
    cycle_ended(dh_exec_ctx)  # the delete hides A's row in its own cycle
    assert latest.shown(dh_to_pylist) == [("B", 2)]
    watchlist.add(new_table([string_col("Symbol", ["A"])]))
    cycle_ended(dh_exec_ctx)
    assert latest.shown(dh_to_pylist) == [("B", 2)]  # A's row entered before A rejoined
    latest.publisher.add(stamped("A", 3))
    wait_for(lambda: latest.shown(dh_to_pylist) == [("A", 3), ("B", 2)])
    # A second row after the rejoin replaces the first: each row is stamped as it arrives, not
    # once per key.
    latest.publisher.add(stamped("A", 4))
    wait_for(lambda: latest.shown(dh_to_pylist) == [("A", 4), ("B", 2)])
    latest.release(dh_exec_ctx)


def test_latest_rows_since_added_keeps_symbols_swapped_between_watchlist_rows(
    dh_exec_ctx: ExecutionContext,
    dh_to_pylist: Callable[[Table], list[dict[str, object]]],
    dh_input_table: Callable[..., InputTable],
) -> None:
    # One write moves A and B between two rows: neither leaves the watchlist, so neither
    # rejoins, and both keep their rows.
    with update_graph.shared_lock(dh_exec_ctx.update_graph):
        watchlist = dh_input_table(
            {"Slot": dtypes.int32, "Symbol": dtypes.string}, key_cols=["Slot"]
        )
    watchlist.add(new_table([int_col("Slot", [1, 2]), string_col("Symbol", ["A", "B"])]))
    latest = latest_of(watchlist)
    latest.publisher.add(stamped("A", 1))
    latest.publisher.add(stamped("B", 2))
    wait_for(lambda: latest.shown(dh_to_pylist) == [("A", 1), ("B", 2)])
    watchlist.add(new_table([int_col("Slot", [1, 2]), string_col("Symbol", ["B", "A"])]))
    cycle_ended(dh_exec_ctx)
    assert latest.shown(dh_to_pylist) == [("A", 1), ("B", 2)]
    latest.release(dh_exec_ctx)


def test_latest_rows_since_added_by_symbol_and_exchange_shows_only_rows_since_the_rejoin(
    dh_exec_ctx: ExecutionContext,
    dh_to_pylist: Callable[[Table], list[dict[str, object]]],
    dh_input_table: Callable[..., InputTable],
) -> None:
    watchlist = keyed_watchlist(["A"], dh_input_table=dh_input_table)
    latest = latest_of(watchlist, ("Symbol", "Exchange"))

    def shown() -> list[tuple[object, object]]:
        return sorted((r["Exchange"], r["Price"]) for r in dh_to_pylist(latest.kept))

    latest.publisher.add(stamped("A", 1, exchange=1))
    latest.publisher.add(stamped("A", 2, exchange=2))
    wait_for(lambda: shown() == [(1, 1), (2, 2)])
    watchlist.delete(new_table([string_col("Symbol", ["A"])]))
    wait_for(lambda: shown() == [])
    watchlist.add(new_table([string_col("Symbol", ["A"])]))
    latest.publisher.add(stamped("A", 5, exchange=2))
    wait_for(lambda: shown() == [(2, 5)])
    latest.release(dh_exec_ctx)


def test_latest_rows_since_added_builds_under_the_consoles_lock(
    dh_exec_ctx: ExecutionContext,
    dh_to_pylist: Callable[[Table], list[dict[str, object]]],
    dh_input_table: Callable[..., InputTable],
) -> None:
    watchlist = keyed_watchlist(["A"], dh_input_table=dh_input_table)
    with update_graph.exclusive_lock(dh_exec_ctx.update_graph):  # as a console command holds it
        latest = latest_of(watchlist)
    latest.publisher.add(stamped("A", 1))
    wait_for(lambda: latest.shown(dh_to_pylist) == [("A", 1)])
    latest.release(dh_exec_ctx)


@pytest.fixture
def parallel_updates() -> Iterator[None]:
    # A smoke test: ask Deephaven to run every `select` and `update` on its parallel threads,
    # then restore the setting, so a formula that reads the cycle number without an execution
    # context there may show a clock frozen at its first step. A pass does not prove the
    # formula ran on those threads.
    # The setting is package-private, so it is opened by reflection. Reflection checks its
    # caller, and a call straight from Python has no Java caller (Java 17 on Linux then raises
    # `NullPointerException`), so a formula, compiled Java, finds and opens the field.
    opened = empty_table(1).update(
        [
            (
                "Field = java.lang.Class.forName(`io.deephaven.engine.table.impl.QueryTable`)"
                ".getDeclaredField(`FORCE_PARALLEL_SELECT_AND_UPDATE`)"
            ),
            "Opened = Field.trySetAccessible()",
        ]
    )
    assert opened.j_table.getColumnSource("Opened").getBoolean(0)
    field = _JPY.cast(
        opened.j_table.getColumnSource("Field").get(0), _JPY.get_type("java.lang.reflect.Field")
    )
    before = field.getBoolean(None)
    field.set(None, _JAVA_BOOLEAN.TRUE)
    try:
        yield
    finally:
        field.setBoolean(None, before)


@pytest.mark.usefixtures("parallel_updates")
def test_latest_rows_since_added_stamps_the_cycle_on_parallel_update_threads(
    dh_exec_ctx: ExecutionContext,
    dh_to_pylist: Callable[[Table], list[dict[str, object]]],
    dh_input_table: Callable[..., InputTable],
) -> None:
    watchlist = keyed_watchlist(["A"], dh_input_table=dh_input_table)
    pins: list[LivenessScope] = []
    scope = new_scope()
    with scope.open():
        blink, publisher = blink_publisher(
            "parallel", EXCHANGED, on_shutdown=lambda: None, release_later=pins.append
        )
        with update_graph.shared_lock(dh_exec_ctx.update_graph):
            built = update_cycle(dh_exec_ctx)
            added = watchlist.select_distinct("Symbol").update(f"Step = {CURRENT_STEP}")
            rows = blink.update(f"Step = {CURRENT_STEP}").last_by("Symbol")
        kept = latest_rows_since_added(blink, watchlist, ("Symbol",))
    publisher.add(stamped("A", 1))
    wait_for(lambda: len(dh_to_pylist(kept)) == 1)
    (joined,) = dh_to_pylist(added)
    (row,) = dh_to_pylist(rows)
    assert joined["Step"] == built > 0
    assert cast("int", row["Step"]) > built
    release_scope(dh_exec_ctx, scope)
    for pin in pins:
        release_scope(dh_exec_ctx, pin)


def _filter_after_last_by(table: Table, watchlist: Table) -> Table:
    return watched_rows(table.last_by("Symbol"), watchlist)


def _latest_rows(table: Table, watchlist: Table) -> Table:
    return latest_rows_since_added(table, watchlist, ("Symbol",))


@pytest.mark.parametrize(
    "keep", [_filter_after_last_by, _latest_rows], ids=["watched_rows", "latest_rows_since_added"]
)
@pytest.mark.parametrize(
    "order", ["in one cycle", "watchlist first", "table first"], ids=lambda order: order
)
def test_a_watchlist_filter_takes_one_failure_when_both_of_its_tables_fail(
    dh_exec_ctx: ExecutionContext, order: str, keep: Callable[[Table, Table], Table]
) -> None:
    # `where_in` would end the process in the first two orders: Deephaven 42.7 delivers a
    # failure of each of its tables to its result, and asserts on the second. Failing both
    # under the update graph's exclusive lock lands them in one cycle, which does not help it.
    pins: list[LivenessScope] = []
    scope = new_scope()
    with scope.open():
        blink, table_publisher = blink_publisher(
            "prices", STAMPED, on_shutdown=lambda: None, release_later=pins.append
        )
        watch_blink, watch_publisher = blink_publisher(
            "watch", {"Symbol": dtypes.string}, on_shutdown=lambda: None, release_later=pins.append
        )
        kept = keep(blink, blink_to_append_only(watch_blink))
        downstream = kept.update_view("Twice = Price * 2")
    if order == "in one cycle":
        with update_graph.exclusive_lock(dh_exec_ctx.update_graph):
            watch_publisher.publish_failure(RuntimeError("the watchlist failed"))
            table_publisher.publish_failure(RuntimeError("the table failed"))
    else:
        first, second = (
            (watch_publisher, table_publisher)
            if order == "watchlist first"
            else (table_publisher, watch_publisher)
        )
        first.publish_failure(RuntimeError("the first failure"))
        wait_for(lambda: bool(kept.j_table.isFailed()))
        second.publish_failure(RuntimeError("the second failure"))
    wait_for(lambda: bool(blink.j_table.isFailed() and watch_blink.j_table.isFailed()))
    with update_graph.shared_lock(dh_exec_ctx.update_graph):
        pass  # the cycle that delivered the last failure has ended, and the process lives
    assert downstream.j_table.isFailed()
    for pin in pins:
        release_scope(dh_exec_ctx, pin)
    release_scope(dh_exec_ctx, scope)
