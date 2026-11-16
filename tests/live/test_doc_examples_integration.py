"""Live runs of the user docs' examples, each as written, checked for what depends on Massive.

Every example is read from its page when the test runs (`tests/_live/doc_examples.py`), so the
code that runs is the code a reader pastes. The examples of a group run one after the other in
one console (`doc_examples.Console`), as a reader pastes them: one command per block, under the
update graph's exclusive lock, each later example rebinding the names of the one before, such
as `m` and `latest`. The test keeps only the tables it checks, so a live table must keep
running after the session that made it is no longer reachable.

These tests assert what only Massive can show: that the tables fill with real data, with
plausible counts; that a reader with no setting gets them filled whatever limit Massive sets;
and what each stock session sends. What the library does with the data, such as Example 2's
ranking or Example 13's closed-session error, `tests/test_docs.py` checks against the fake
servers. The groups follow the README:

- the hook and Examples 1 to 6 and 13, with no setting, and again paced by a declared limit;
- Examples 7 to 12, which load the settings file Example 7 saves, checked against what the
  stock session of the moment sends: pre-market, regular, after hours, or overnight;
- the streaming example's crypto line;
- the tester guide's example 10, which describes a key Massive refuses the live minute bars
  and the bars of 2015 to.

A test whose data Massive refuses the key skips, quoting Massive.

A claim that needs another stock session than the current one goes unchecked, and the evidence
names the session. Each test records what it saw (`evidence`). How to run these tests is in
`docs/live-tests.md`.
"""

from __future__ import annotations

import dataclasses
import functools
import gc
import itertools
import re
import threading
import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final

import pytest
from deephaven import update_graph

import deephaven_massive as dm
from deephaven_massive._ingest import rate_limits
from tests._live import doc_examples, streams
from tests._live.calendar import NEW_YORK, is_trading_day
from tests._live.tester.examples import guide_blocks

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from typing import TypeAlias

    from deephaven.execution_context import ExecutionContext
    from deephaven.table import Table

pytestmark = pytest.mark.integration

# Fixture types, repeated in each live module: test modules may not import from conftest.
TableRows: TypeAlias = "Callable[[Table], list[dict[str, Any]]]"
Evidence: TypeAlias = "Callable[[str], None]"
StreamReport: TypeAlias = "Callable[[dm.MassiveSession], list[str]]"
SkipIfNotEntitled: TypeAlias = "Callable[[dm.MassiveSession], None]"
WaitAcked: TypeAlias = "Callable[[dm.MassiveSession, int, float], int]"
ConsoleCommand: TypeAlias = "Callable[..., None]"
Names: TypeAlias = "dict[str, Any]"
Rows: TypeAlias = "list[dict[str, Any]]"

_ROOT = Path(__file__).resolve().parent.parent.parent
README = _ROOT / "README.md"
TESTER_GUIDE = _ROOT / "docs" / "tester-guide.md"

_CONSOLE_WAIT: Final = 120.0
"""Seconds one console command may take: a read of a subscription Massive limits waits up to
90 s for its pace under the console's lock before it gives up."""

_FILL_WAIT: Final = 90.0
"""Seconds a poll's first rows may take, a paced request's wait for its minute included."""

_ACK_WAIT: Final = 30.0
"""Seconds a stream may take to be acknowledged."""

_REFUSAL_WAIT: Final = 60.0
"""Seconds a refused stream may take to fail."""

_MINUTES_WATCHED: Final = 3
"""Minute boundaries the streaming examples are watched across."""

_BAR_COLUMNS: Final = ("Date", "Open", "High", "Low", "Close", "Volume")

_CRYPTO_LINE: Final = re.compile(r"`(m\.crypto\.bars\([^`]*\)\.last_by\(\))`")
"""The streaming example's line for crypto, inline in its text."""

_STOCKS: Final = frozenset({"AAPL", "MSFT", "NVDA"})
"""The stocks Examples 3, 7, 8, 9, and 12 read."""


def _wait(check: Callable[[], bool], seconds: float) -> bool:
    """Wait until a check passes, or the seconds pass.

    Args:
        check: The condition.
        seconds: The most seconds to wait.

    Returns:
        Whether the check passed.
    """
    deadline = time.monotonic() + seconds
    pause = threading.Event()  # never set; only paces the polling
    while not check():
        if time.monotonic() >= deadline:
            return False
        pause.wait(0.5)
    return True


def _sleep_until(moment: datetime) -> None:
    """Wait until a moment of the wall clock.

    Args:
        moment: An aware instant.
    """
    seconds = (moment - datetime.now(UTC)).total_seconds()
    if seconds > 0:
        threading.Event().wait(seconds)


def _symbols(rows: Sequence[dict[str, Any]]) -> set[str]:
    """Return the `Symbol` of each row.

    Returns:
        The symbols.
    """
    return {str(row["Symbol"]) for row in rows}


@dataclasses.dataclass
class _Run:
    """One group's console, and where its progress goes.

    Attributes:
        console: The console the group's examples run in.
        evidence: Records when each example ran.
        started: When the group started, by `time.monotonic()`.
    """

    console: doc_examples.Console
    evidence: Evidence
    started: float = dataclasses.field(default_factory=time.monotonic)

    def paste(self, blocks: Sequence[doc_examples.Block], name: str, *keep: str) -> Names:
        """Run blocks in the console, and return the names asked for.

        Args:
            blocks: The blocks.
            name: The example, for the evidence.
            keep: The names to return.

        Returns:
            Each kept name's value.
        """
        kept = self.console.run(blocks, *keep)
        self.evidence(f"{name}: ran by {time.monotonic() - self.started:.0f} s")
        return kept

    def section(self, anchor: str, *keep: str) -> Names:
        """Run the Python blocks of one README section, and return the names asked for.

        Args:
            anchor: The section's `<a id>`.
            keep: The names to return.

        Returns:
            Each kept name's value.
        """
        return self.paste(doc_examples.section_blocks(README, anchor), anchor, *keep)


def _console(tmp_path: Path, console_command: ConsoleCommand, evidence: Evidence) -> _Run:
    """Open one console whose commands may wait out a limited subscription's pace.

    Returns:
        The console.
    """
    command = functools.partial(console_command, timeout=_CONSOLE_WAIT)
    return _Run(doc_examples.Console(tmp_path, command), evidence)


# --- The hook and Examples 1 to 6 and 13 ----------------------------------------------------------


@pytest.mark.jvm_held  # the examples' formulas hand their globals, the sessions too, to the JVM
@pytest.mark.timeout(1200)
def test_the_free_examples_fill_with_no_setting(
    live_config: dm.MassiveConfig,
    *,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    console_command: ConsoleCommand,
    table_rows: TableRows,
    skip_if_not_entitled: SkipIfNotEntitled,
    evidence: Evidence,
) -> None:
    """Whether the hook and Examples 1 to 6 and 13 fill with no setting, by a new reader's path.

    Runs them back to back in one console with the main key and no setting, as a new reader
    does. Their sessions get a rate governor of their own, as in a new server, not the run's,
    which paces from the first request what the owner's settings say Massive limits: so when
    Massive limits stocks, it refuses the first request past its minute's count, and each
    session learns the limit, pauses, and resumes inside the console's patience. The examples
    send about 15 stock requests. Records from `m.rate_limits()` of a session on the same key
    whether the stocks governor learned a limit.

    Asserts what `_run_free_examples` checks.
    """
    monkeypatch.setattr(rate_limits, "GOVERNORS", rate_limits.GovernorRegistry())
    evidence("a governor of the test's own and no setting: a limit is learned from Massive")
    run = _console(tmp_path, console_command, evidence)
    try:
        with doc_examples.recorded_sessions():
            _run_free_examples(
                run,
                table_rows=table_rows,
                skip_if_not_entitled=skip_if_not_entitled,
                evidence=evidence,
            )
            _record_stocks_limit(live_config, table_rows=table_rows, evidence=evidence)
    finally:
        run.console.close()


def _record_stocks_limit(
    live_config: dm.MassiveConfig, *, table_rows: TableRows, evidence: Evidence
) -> None:
    """Record whether the key's stocks governor learned a limit, as `m.rate_limits()` shows it.

    Opens a session with the key alone while the examples' sessions still hold the governor,
    so its row is theirs.
    """
    session = dm.MassiveSession(dm.MassiveConfig(api_key=live_config.api_key))
    try:
        view = session.rate_limits()
        rows = [row for row in table_rows(view) if row["Bucket"] == "stocks"]
        session.release(view)
    finally:
        session.close()
    evidence(f"stocks rate limit after the examples: {rows or 'no row'}")


@pytest.mark.jvm_held  # the examples' formulas hand their globals, the sessions too, to the JVM
@pytest.mark.timeout(900)
def test_the_free_examples_fill_back_to_back_at_a_declared_pace(
    live_config: dm.MassiveConfig,
    *,
    tmp_path: Path,
    console_command: ConsoleCommand,
    table_rows: TableRows,
    skip_if_not_entitled: SkipIfNotEntitled,
    evidence: Evidence,
) -> None:
    """Whether the hook and Examples 1 to 6 and 13 fill with real data, at a declared pace.

    Runs them back to back in one console while a session that declares stocks limited
    (`rest_requests_per_minute`, a user setting: the owner's stocks limit, else 5) holds the
    key's rate governor, so every session's stock requests keep to it. The setting paces from
    the first request, so this test never exercises Massive's refusal of a request past its
    limit and the pause after it; `test_the_free_examples_fill_with_no_setting` does, when
    Massive limits stocks.

    Asserts what `_run_free_examples` checks.
    """
    evidence("paced by the setting: Massive's refusal of a request past its limit is not exercised")
    limited = {dm.Subscription.STOCKS: 5, **live_config.rest_requests_per_minute}
    pacer = dm.MassiveSession(dataclasses.replace(live_config, rest_requests_per_minute=limited))
    run = _console(tmp_path, console_command, evidence)
    try:
        with doc_examples.recorded_sessions():
            _run_free_examples(
                run,
                table_rows=table_rows,
                skip_if_not_entitled=skip_if_not_entitled,
                evidence=evidence,
            )
    finally:
        run.console.close()
        pacer.close()


def _run_free_examples(
    run: _Run,
    *,
    table_rows: TableRows,
    skip_if_not_entitled: SkipIfNotEntitled,
    evidence: Evidence,
) -> None:
    """Run the hook and Examples 1 to 6 and 13 in one console, and check what Massive sent.

    Checks: the hook's year of Apple's bars; Example 1's five daily bars with their columns;
    Example 2's day of more than 1,000 stocks, none over the counter, and its top 20;
    Example 3's three returns; Example 4's year of Nvidia's bars; Example 5's headlines for
    each of its stocks, at most ten each, the limit Massive applies per ticker; Example 6's
    four previous closes, and the previous closes of the three Yahoo tickers that end it,
    under Massive's spellings. Example 13 and Example 6's edit run as written; what they do is
    the library's, which `tests/test_docs.py` checks. Each example reads in the background,
    so Massive's refusal fails its table instead of raising: after each wait, a refusal in its
    session's `m.streams()` skips the test, quoting Massive, before its rows are checked.
    """
    ran = {"hook": run.paste(doc_examples.intro_blocks(README), "hook", "m", "apple")}
    keep = {
        "read-daily-bars": ("m", "apple"),
        "whole-market-day": ("m", "day", "busiest"),
        "compare-stocks": ("m", "returns"),
        "chart-closes": ("m", "nvda"),
    }
    for anchor, names in keep.items():
        ran[anchor] = run.section(anchor, *names)
    _check_history_examples(
        ran, table_rows=table_rows, skip_if_not_entitled=skip_if_not_entitled, evidence=evidence
    )
    news = run.section("follow-the-news", "m", "headlines")
    _check_news(
        news, table_rows=table_rows, skip_if_not_entitled=skip_if_not_entitled, evidence=evidence
    )
    opening, edit = doc_examples.section_blocks(README, "drive-with-a-watchlist")
    watched = run.paste([opening], "drive-with-a-watchlist", "m", "closes")
    closes = watched["closes"]
    first = {"AAPL", "MSFT", "AMZN", "GOOGL"}
    filled = _wait(lambda: _symbols(table_rows(closes)) == first, _FILL_WAIT)
    evidence(f"example 6: {sorted(_symbols(table_rows(closes)))}, filled: {filled}")
    skip_if_not_entitled(watched["m"])
    assert filled, f"Example 6's closes did not fill within {_FILL_WAIT:.0f} s"
    run.paste([edit], "drive-with-a-watchlist edit")
    converting = run.section("convert-symbols", "m", "yahoo_closes")
    yahoo = converting["yahoo_closes"]
    converted = {"AAPL", "BRK.B", "BACpL"}
    filled = _wait(lambda: _symbols(table_rows(yahoo)) == converted, _FILL_WAIT)
    evidence(f"example 6, Yahoo tickers: {sorted(_symbols(table_rows(yahoo)))}, filled: {filled}")
    skip_if_not_entitled(converting["m"])
    assert filled, f"the Yahoo tickers' closes did not fill within {_FILL_WAIT:.0f} s"
    run.section("stop-live-tables")


def _check_history_examples(
    ran: dict[str, Names],
    *,
    table_rows: TableRows,
    skip_if_not_entitled: SkipIfNotEntitled,
    evidence: Evidence,
) -> None:
    """Check the hook and Examples 1 to 4: real bars, in plausible numbers.

    Each reads in the background, so each check first waits for its table to fill, then skips,
    quoting Massive, when Massive refused a read of the example's session.
    """

    def when_filled(
        names: Names, table: Table, enough: Callable[[int], bool]
    ) -> list[dict[str, Any]]:
        _wait(lambda: enough(len(table_rows(table))), _FILL_WAIT)
        skip_if_not_entitled(names["m"])
        return table_rows(table)

    year = when_filled(ran["hook"], ran["hook"]["apple"], lambda rows: rows >= 240)
    evidence(f"hook: {len(year)} daily bars of AAPL")
    assert len(year) >= 240, "the hook's year of Apple's daily bars is short"

    first = ran["read-daily-bars"]["apple"]
    week = when_filled(ran["read-daily-bars"], first, lambda rows: rows == 5)
    evidence(f"example 1: {len(week)} bars, dates {[row['Date'] for row in week]}")
    assert len(week) == 5, "Example 1 reads five trading days"
    assert set(_BAR_COLUMNS) <= set(first.column_names)

    market = ran["whole-market-day"]
    day = when_filled(market, market["day"], lambda rows: rows > 1000)
    otc = [row["Symbol"] for row in day if row.get("Otc")]
    evidence(f"example 2: {len(day)} stocks, {len(otc)} over the counter")
    assert len(day) > 1000, "Example 2's day holds every listed US stock"
    assert not otc, f"Example 2's day holds stocks over the counter: {otc[:5]}"
    assert len(when_filled(market, market["busiest"], lambda rows: rows == 20)) == 20

    compared = ran["compare-stocks"]["returns"]
    filled = _wait(lambda: _symbols(table_rows(compared)) == _STOCKS, _FILL_WAIT)
    skip_if_not_entitled(ran["compare-stocks"]["m"])
    returns = table_rows(compared)
    evidence(f"example 3: {[(r['Symbol'], r['ReturnPct']) for r in returns]}, filled: {filled}")
    assert filled, f"Example 3's returns did not fill within {_FILL_WAIT:.0f} s"

    nvda = when_filled(ran["chart-closes"], ran["chart-closes"]["nvda"], lambda rows: rows >= 240)
    evidence(f"example 4: {len(nvda)} daily bars of NVDA")
    assert len(nvda) >= 240, "Example 4's year of Nvidia's daily bars is short"


def _check_news(
    ran: Names,
    *,
    table_rows: TableRows,
    skip_if_not_entitled: SkipIfNotEntitled,
    evidence: Evidence,
) -> None:
    """Check Example 5: headlines for each stock, at most ten each.

    Skips, quoting Massive, when Massive refused a read of its session.
    """
    asked = {"AAPL", "NVDA", "TSLA"}
    headlines = ran["headlines"]
    filled = _wait(lambda: _symbols(table_rows(headlines)) == asked, _FILL_WAIT)
    skip_if_not_entitled(ran["m"])
    rows = table_rows(headlines)
    counts = {symbol: sum(row["Symbol"] == symbol for row in rows) for symbol in sorted(asked)}
    evidence(f"example 5: articles per stock {counts}, every stock in by the wait: {filled}")
    assert filled, f"Example 5's headlines hold {counts} after {_FILL_WAIT:.0f} s"
    assert all(count <= 10 for count in counts.values()), f"more than ten for a stock: {counts}"


# --- Examples 7 to 12, on a key Massive streams stocks to ----------------------------------------


@pytest.mark.jvm_held  # the examples' formulas hand their globals, the sessions too, to the JVM
@pytest.mark.timeout(600)
def test_the_paid_examples_fill_in_the_current_stock_session(
    live_config: dm.MassiveConfig,
    live_session: dm.MassiveSession,
    *,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    console_command: ConsoleCommand,
    dh_exec_ctx: ExecutionContext,
    wait_acked: WaitAcked,
    table_rows: TableRows,
    stream_report: StreamReport,
    skip_if_not_entitled: SkipIfNotEntitled,
    evidence: Evidence,
) -> None:
    """Whether Examples 7 to 12 fill with what the stock session of the moment sends.

    Saves the settings file with the README's commands, listing stocks as delayed when the
    owner's settings do, in a stand-in home folder, runs Examples 7 to 12 in one console,
    in order, and collects whatever the console no longer reaches, such as the sessions each
    later example's `m` replaced. Then watches the streams across three minute boundaries.

    Asserts at any hour: Example 7's snapshot of three stocks with its columns, except while
    Massive's snapshots are empty (`doc_examples.snapshots_cleared`, or on the delayed feed,
    whose lag the tests do not know, any empty snapshot); Example 12's three topics
    acknowledged and none of its streams failed. Then, by the session the feed is in
    (`streams.stock_session`: the wall clock on the real-time feed; on the delayed feed, the
    latest bar's end, so the test skips when no bar arrives there):

    - pre-market, regular, or after hours: Example 8's minute bars; Example 10's bars of SPY;
      bars reaching Example 12's chart. In the regular session, also: every stock has its
      bar, Example 8's rows change each minute, the chart's stream grows each minute, and
      Example 11's `spy` holds every minute REST has of the day so far across the seam with
      the stream (`doc_examples.seam_gaps`). Outside it, REST may hold minutes made only of
      trades reported late, which no live bar had, so those minutes are recorded but not
      required.
    - overnight: Example 8's `latest` and the other live tables stay empty, and `spy` holds
      the bars REST has of the day Example 11 read, or none on a day the market is closed.

    A run whose feed crosses from one session to another while it watches is skipped after
    the checks that hold at any hour. When Massive refuses Example 7's snapshot or Example
    12's streams, whose tables fail in the background, the test skips, quoting Massive,
    before checking that example; so too when the library refuses them as delayed data the
    settings file does not list, quoting the library.
    """
    monkeypatch.setenv("HOME", str(tmp_path))
    delayed = dm.Subscription.STOCKS in live_config.delayed_subscriptions
    doc_examples.write_settings_file(README, tmp_path, delayed=delayed)
    evidence(f"settings file for the {'delayed' if delayed else 'real-time'} feed")
    run = _console(tmp_path, console_command, evidence)

    def locked_rows(*tables: Table) -> list[Rows]:
        with update_graph.shared_lock(dh_exec_ctx.update_graph):
            return [table_rows(table) for table in tables]

    try:
        with doc_examples.recorded_sessions():
            snapshot = run.section("what-your-plan-allows", "m", "snapshot")
            _check_snapshot(
                snapshot,
                delayed=delayed,
                table_rows=table_rows,
                skip_if_not_entitled=skip_if_not_entitled,
                evidence=evidence,
            )
            snapshot.clear()  # Example 8's `m` replaces Example 7's, as in a reader's console
            ran = {"stream-minute-bars": run.section("stream-minute-bars", "latest")}
            ran["change-since-close"] = run.section("change-since-close", "changes")
            ran["table-shapes"] = run.section("table-shapes", "since_start")
            backfill_day = datetime.now(NEW_YORK).date()  # the day the example's dh_today reads
            ran["backfill-then-live"] = run.section("backfill-then-live", "spy", "live")
            ran["dashboard"] = run.section("dashboard", "m")
            gc.collect()  # what the console no longer reaches, such as the replaced sessions
            board = ran["dashboard"]["m"]
            acked = wait_acked(board, len(_STOCKS), time.monotonic() + _ACK_WAIT)
            for line in stream_report(board):
                evidence(line)
            skip_if_not_entitled(board)
            assert acked >= len(_STOCKS), "Example 12's topics were not all acknowledged"
            seen = [
                _watch_minute(
                    ran, locked_rows=locked_rows, table_rows=table_rows, evidence=evidence
                )
                for _ in range(_MINUTES_WATCHED)
            ]
            failed = [r["Error"] for r in table_rows(board.streams()) if r["State"] == "FAILED"]
            assert not failed, f"Example 12's stream failed: {failed}"
            first_session = _feed_session(seen[0], delayed=delayed)
            last_session = _feed_session(seen[-1], delayed=delayed)
            evidence(f"stock session of the feed: {first_session}, then {last_session}")
            if first_session is None or last_session is None:
                pytest.skip("no bar on the delayed feed, whose lag the tests do not know")
            if first_session != last_session:
                pytest.skip(f"the feed moved from {first_session} to {last_session} meanwhile")
            if first_session == streams.OVERNIGHT:
                _check_overnight(
                    seen[-1], backfill_day, live_session, table_rows=table_rows, evidence=evidence
                )
            else:
                _check_streaming(seen, first_session, evidence=evidence)
                _check_seam(
                    ran["backfill-then-live"],
                    backfill_day,
                    live_session,
                    strict=first_session == streams.REGULAR,
                    locked_rows=locked_rows,
                    table_rows=table_rows,
                    evidence=evidence,
                )
    finally:
        run.console.close()


def _check_snapshot(
    ran: Names,
    *,
    delayed: bool,
    table_rows: TableRows,
    skip_if_not_entitled: SkipIfNotEntitled,
    evidence: Evidence,
) -> None:
    """Check Example 7: a snapshot of three stocks, with the columns its text names.

    The snapshot reads in the background, so a refusal fails its table instead of raising:
    the check waits for rows or a failed read in `m.streams()`, skips on Massive's refusal,
    quoting Massive, or on delayed data the settings file does not list, quoting the library,
    and fails on any other failed read. Massive empties its snapshots
    from 03:30 to 04:00 New York time, and on the delayed feed refills them later by a lag the
    tests do not know, so there an empty snapshot of a finished read is recorded, not failed.

    Args:
        ran: Example 7's `m` and `snapshot`.
        delayed: Whether the settings file lists stocks as delayed.
        table_rows: Snapshots a table into rows.
        skip_if_not_entitled: Skips when Massive or the settings refused a read of the
            session.
        evidence: Records what the snapshot held.
    """
    session, snapshot = ran["m"], ran["snapshot"]
    reads = session.streams()

    def failed() -> list[str]:
        return [str(row["Error"]) for row in table_rows(reads) if row["State"] == "FAILED"]

    def done() -> bool:
        states = [row["State"] for row in table_rows(reads)]
        return bool(states) and all(state == "DONE" for state in states)

    _wait(lambda: bool(failed()) or bool(table_rows(snapshot)) or done(), _FILL_WAIT)
    errors = failed()
    finished = done()
    session.release(reads)
    skip_if_not_entitled(session)
    assert not errors, f"Example 7's snapshot read failed: {errors}"
    assert {"DayClose", "PrevDayClose", "TodaysChangePerc"} <= set(snapshot.column_names)
    rows = table_rows(snapshot)
    evidence(f"example 7: {[(r['Symbol'], r['DayClose']) for r in rows]}")
    if doc_examples.snapshots_cleared(datetime.now(UTC), timedelta(0)):
        evidence("example 7's rows unchecked: Massive's snapshots are empty from 03:30 to 04:00")
        return
    if delayed and not rows:
        assert finished, f"example 7's snapshot read did not finish in {_FILL_WAIT:.0f} s"
        evidence("example 7's rows unchecked: the delayed feed's snapshot is empty")
        return
    assert _symbols(rows) == _STOCKS


def _feed_session(seen: dict[str, Any], *, delayed: bool) -> str | None:
    """Name the stock session the feed is in, by its own time.

    Args:
        seen: One minute's rows of the streaming examples (`_watch_minute`).
        delayed: Whether the examples stream from the delayed feed.

    Returns:
        On the real-time feed, the session of the wall clock when the minute was read
        (`clock`); on the delayed feed, the session of the latest minute bar's end in Example
        8's and 10's tables, or None when no bar has arrived.
    """
    if not delayed:
        return streams.stock_session(seen["clock"])
    ends = [int(r["TimestampEnd"]) for r in (*seen["latest"], *seen["since_start"])]
    if not ends:
        return None
    return streams.stock_session(datetime.fromtimestamp(max(ends) / 1e9, UTC))


def _watch_minute(
    ran: dict[str, Names],
    *,
    locked_rows: Callable[..., list[Rows]],
    table_rows: TableRows,
    evidence: Evidence,
) -> dict[str, Any]:
    """Wait past the next minute boundary, then read every streaming example's tables.

    Args:
        ran: The tables each example made, by its README anchor.
        locked_rows: Reads tables together, under the update graph's shared lock.
        table_rows: Reads one table.
        evidence: Records the counts.

    Returns:
        Each table's rows, by name; `published`, the rows the dashboard's stream published;
        and `clock`, the wall clock as the tables were read.
    """
    _sleep_until(streams.minute_bar_deadline(datetime.now(UTC)))
    clock = datetime.now(UTC)
    names = ("latest", "changes", "since_start", "spy")
    tables = (
        ran["stream-minute-bars"]["latest"],
        ran["change-since-close"]["changes"],
        ran["table-shapes"]["since_start"],
        ran["backfill-then-live"]["spy"],
    )
    seen: dict[str, Any] = dict(zip(names, locked_rows(*tables), strict=True))
    seen["clock"] = clock
    seen["published"] = sum(
        int(row["RowsPublished"] or 0)
        for row in table_rows(ran["dashboard"]["m"].streams())
        if row["Dataset"] == "stocks.bars"
    )
    evidence(
        "minute: "
        + ", ".join(f"{name} {len(seen[name])}" for name in names)
        + f", dashboard stream {seen['published']} rows"
    )
    return seen


def _check_streaming(seen: list[dict[str, Any]], session: str, *, evidence: Evidence) -> None:
    """Check Examples 8 to 12 while the stock market trades."""
    last = seen[-1]
    evidence(f"checked in the {session} session")
    assert last["latest"], f"no minute bar of Example 8 arrived in the {session} session"
    assert last["since_start"], "Example 10's since_start holds no bar"
    published = [minute["published"] for minute in seen]
    assert published[-1] > 0, "Example 12's chart got no bar"
    if session != streams.REGULAR:
        return
    assert _symbols(last["latest"]) == _STOCKS
    ends = {row["Symbol"]: row["TimestampEnd"] for row in seen[0]["latest"]}
    moved = [r["Symbol"] for r in last["latest"] if r["TimestampEnd"] != ends.get(r["Symbol"])]
    assert moved, "no row of Example 8's latest changed as the minutes ended"
    assert all(row["Close"] is not None for row in last["changes"]), last["changes"]
    grew = all(later > earlier for earlier, later in itertools.pairwise(published))
    assert grew, f"Example 12's chart did not grow each minute: {published}"


def _rest_minutes(live_session: dm.MassiveSession, table_rows: TableRows, day: date) -> set[int]:
    """Return the start of every minute bar REST has of SPY on a day, as epoch nanoseconds.

    Returns:
        The starts.
    """
    history = live_session.stocks.bars("SPY", dm.Timespan.MINUTE).history(
        day.isoformat(), wait=True
    )
    return {int(row["TimestampStart"]) for row in table_rows(history)}


def _check_seam(
    backfill: Names,
    day: date,
    live_session: dm.MassiveSession,
    *,
    strict: bool,
    locked_rows: Callable[..., list[Rows]],
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    """Check Example 11: every minute REST has of the day so far, none twice, across the seam.

    Args:
        backfill: Example 11's `spy` and `live`.
        day: The day Example 11 read.
        live_session: The session that reads REST's minutes again.
        strict: Whether every REST minute must be in `spy`: in the regular session only, since
            outside it REST may hold minutes made only of trades reported late.
        locked_rows: Reads tables together, under the update graph's shared lock, so `spy`
            and `live` hold the same live bars.
        table_rows: Reads REST's minutes.
        evidence: Records the comparison.
    """
    spy_rows, live_rows = locked_rows(backfill["spy"], backfill["live"])
    spy = [int(row["TimestampStart"]) for row in spy_rows]
    live = [int(row["TimestampStart"]) for row in live_rows]
    assert len(spy) == len(set(spy)), "Example 11's spy holds a minute twice"
    assert live, "no live bar reached Example 11"
    missing, extra = doc_examples.seam_gaps(spy, live, _rest_minutes(live_session, table_rows, day))
    evidence(
        f"example 11: {len(spy)} minutes, {len(live)} live; missing {missing[:5]} "
        f"({len(missing)}); only in spy {extra[:5]}; required: {strict}"
    )
    if strict:
        assert not missing, f"Example 11's spy lacks {len(missing)} minutes that REST has"


def _check_overnight(
    last: dict[str, Any],
    day: date,
    live_session: dm.MassiveSession,
    *,
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    """Check Examples 8 to 11 between sessions: the live tables empty, `spy` only history."""
    evidence("checked overnight")
    assert not last["latest"], "Example 8's latest filled overnight"
    assert not last["since_start"], "Example 10's since_start filled overnight"
    assert all(row["Close"] is None for row in last["changes"]), last["changes"]
    spy = {int(row["TimestampStart"]) for row in last["spy"]}
    if not is_trading_day(day):
        evidence(f"example 11 on {day}, a closed day: {len(spy)} rows")
        assert not spy, "Example 11's spy holds bars on a day the market is closed"
        return
    rest = _rest_minutes(live_session, table_rows, day)
    evidence(f"example 11 overnight, {day}: spy {len(spy)} minutes, REST {len(rest)}")
    assert spy == rest, "Example 11's spy does not hold the day's minutes from history"


# --- Single examples ----------------------------------------------------------------------------


@pytest.mark.timeout(150)
def test_the_streaming_examples_crypto_line_streams_now(
    *,
    tmp_path: Path,
    console_command: ConsoleCommand,
    wait_acked: WaitAcked,
    table_rows: TableRows,
    stream_report: StreamReport,
    skip_if_not_entitled: SkipIfNotEntitled,
    evidence: Evidence,
) -> None:
    """Whether the streaming example's crypto line fills, at any hour.

    Runs the line as the example's text gives it, in a session opened as the README's examples
    open theirs, and waits for the first minute bar past the next minute boundary. Passes when
    Massive acknowledged the topic and a bar arrived; skips, quoting Massive, when Massive
    refuses the stream.
    """
    found = _CRYPTO_LINE.search(doc_examples.section(README, "stream-minute-bars"))
    assert found, "the streaming example's text has no crypto line"
    code = f"import deephaven_massive as dm\n\nm = dm.MassiveSession()\nlatest = {found.group(1)}\n"
    run = _console(tmp_path, console_command, evidence)
    try:
        with doc_examples.recorded_sessions():
            names = run.paste([doc_examples.Block(code, continues=False)], "crypto", "m", "latest")
            acked = wait_acked(names["m"], 1, time.monotonic() + _ACK_WAIT)
            deadline = streams.minute_bar_deadline(datetime.now(UTC)) + timedelta(seconds=60)
            filled = _wait(
                lambda: bool(table_rows(names["latest"])),
                (deadline - datetime.now(UTC)).total_seconds(),
            )
            for line in stream_report(names["m"]):
                evidence(line)
            evidence(f"{found.group(1)}: acknowledged {acked}, rows {table_rows(names['latest'])}")
            skip_if_not_entitled(names["m"])
            names.clear()
    finally:
        run.console.close()
    assert acked, "Massive did not acknowledge the crypto line's topic"
    assert filled, "the crypto line's latest stayed empty"


_TESTER_LINES: Final = (
    "the latest trade: EntitlementError: ",
    "live minute bars: opened; ",
    "bars from 2015: EntitlementError: ",
)
"""The starts of the three lines the tester guide's example 10 prints, as it says, on a key
Massive refuses the live minute bars and the bars of 2015 to."""

_SERVED_2015: Final = "bars from 2015: no error"
"""The start of the line example 10 prints when Massive served the bars of 2015."""

_REFUSED_TOPIC: Final = "AM.AAPL"
_REJECTED_KEY: Final = "not only a bad key"
"""How a failed stream's error quotes Massive's refusal of the topic, and how it says Massive
rejected the key, the guide's two outcomes."""


@pytest.mark.requests(2)
@pytest.mark.timeout(240)
def test_tester_example_10_fails_the_live_minute_bars_where_massive_refuses_them(
    *,
    tmp_path: Path,
    console_command: ConsoleCommand,
    table_rows: TableRows,
    capsys: pytest.CaptureFixture[str],
    evidence: Evidence,
) -> None:
    """Whether the tester guide's example 10 prints its three lines and fails its live bars.

    Runs the example as the guide gives it with the main key. It sends the key's stocks two
    requests, each of which may wait out a limited subscription's pace inside the console's
    patience. The guide describes a key Massive refuses the bars of 2015 and the live minute
    bars to. Whatever Massive answered, the example must print its three lines (the latest
    trade, the live minute bars, the bars from 2015), and its `stocks.bars` stream must settle:
    `FAILED`, or running with its topic acknowledged or rows published; a missing line or an
    unsettled stream fails the test. When Massive served either the bars of 2015 (the
    example's line for them says no error) or the live bars (the stream runs), the test skips,
    saying what Massive served. Otherwise it passes when the latest trade and the bars from
    2015 are refused with `EntitlementError`, the live bars opened, and the `stocks.bars` row
    of `streams` reads `FAILED` with one of the guide's two outcomes: Massive's refusal of
    `AM.AAPL`, or Massive's rejection of the key.
    """
    code = guide_blocks(TESTER_GUIDE.read_text(encoding="utf-8"))[10]
    run = _console(tmp_path, console_command, evidence)

    def bars(streams_table: Table) -> Rows:
        return [r for r in table_rows(streams_table) if r["Dataset"] == "stocks.bars"]

    try:
        with doc_examples.recorded_sessions():
            names = run.paste(
                [doc_examples.Block(code, continues=False)], "tester 10", "streams", "connections"
            )
            printed = capsys.readouterr().out.splitlines()
            for line in printed:
                evidence(f"printed: {line}")
            _wait(
                lambda: any(r["State"] == "FAILED" for r in bars(names["streams"])), _REFUSAL_WAIT
            )
            rows = bars(names["streams"])
            connections = table_rows(names["connections"])
            for row in connections:
                evidence(
                    f"connection {row['AssetClass']} {row['State']} acked {row['AckedTopics']} "
                    f"{row['LastError']!r}"
                )
            names.clear()
    finally:
        run.console.close()
    evidence(f"stocks.bars: {[(row['State'], row['Error']) for row in rows]}")
    for start in _TESTER_LINES:
        label = start.split(": ")[0]
        assert any(line.startswith(f"{label}: ") for line in printed), f"no {label!r} line"
    acked = sum(int(r["AckedTopics"]) for r in connections if r["AssetClass"] == "stocks")
    states = [row["State"] for row in rows]
    live = states == ["RUNNING"] and bool(acked or rows[0]["RowsPublished"])
    assert live or states == ["FAILED"], f"stocks.bars settled neither way: {states}"
    served = []
    if any(line.startswith(_SERVED_2015) for line in printed):
        served.append("the bars from 2015")
    if live:
        served.append("the live minute bars")
    if served:
        pytest.skip(
            f"Massive served {' and '.join(served)}; the guide's example 10 describes a key "
            "Massive refuses them to"
        )
    for start in _TESTER_LINES:
        assert any(line.startswith(start) for line in printed), f"no line starts {start!r}"
    assert [row["State"] for row in rows] == ["FAILED"]
    error = str(rows[0]["Error"])
    assert _REFUSED_TOPIC in error or _REJECTED_KEY in error, error
