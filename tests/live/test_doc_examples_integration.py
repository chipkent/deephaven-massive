"""Live runs of the user docs' examples, each as written, checked for what depends on Massive.

Every example is read from its page when the test runs (`tests/_live/doc_examples.py`), so the
code that runs is the code a reader pastes. The examples of a group run one after the other in
one console (`doc_examples.Console`), as a reader pastes them: one command per block, under the
update graph's exclusive lock, each later example rebinding the names of the one before, such
as `m` and `latest`. The test keeps only the tables it checks, so a live table must keep
running after the session that made it is no longer reachable.

These tests assert what only Massive can show: that the tables fill with real data, with
plausible counts; that Basic's rate limit and a delayed plan behave as the README says; and
what each stock session sends. What the library does with the data, such as Example 2's ranking
or Example 14's closed-session error, `tests/test_docs.py` checks against the fake servers. The
groups follow the README:

- the hook and Examples 1 to 6, 13, and 14, on a main key on Stocks Basic, or paced at
  Basic's rate on a paid one;
- Examples 7 to 12, which load the settings file Example 7 saves, checked against what the
  stock session of the moment sends: pre-market, regular, after hours, or overnight;
- Example 7 without its setting, on a delayed Stocks plan;
- the streaming example's crypto line, on Currencies Starter;
- the tester guide's example 10, on a main key on Stocks Basic.

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
import runpy
import threading
import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final

import pytest
from deephaven import update_graph

import deephaven_massive as dm
from deephaven_massive._ingest import rate_limits
from tests._live import doc_examples, plans, streams
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
WaitAcked: TypeAlias = "Callable[[dm.MassiveSession, int, float], int]"
ConsoleCommand: TypeAlias = "Callable[..., None]"
Names: TypeAlias = "dict[str, Any]"
Rows: TypeAlias = "list[dict[str, Any]]"

_ROOT = Path(__file__).resolve().parent.parent.parent
README = _ROOT / "README.md"
TESTER_GUIDE = _ROOT / "docs" / "tester-guide.md"

_CONSOLE_WAIT: Final = 120.0
"""Seconds one console command may take: a read paced at Basic's rate waits up to 90 s under
the console's lock before it gives up."""

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

_DELAYED_REFUSAL: Final = "is not in MassiveConfig.delayed_subscriptions"
"""What the library's refusal of delayed data says about the setting that would accept it."""

_STOCKS: Final = frozenset({"AAPL", "MSFT", "NVDA"})
"""The stocks Examples 3, 7, 8, 9, and 12 read."""

_ON_BASIC: Final = "needs the main key on Stocks Basic, as a free tester's is"


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
    """Open one console whose commands may wait out Basic's pace.

    Returns:
        The console.
    """
    command = functools.partial(console_command, timeout=_CONSOLE_WAIT)
    return _Run(doc_examples.Console(tmp_path, command), evidence)


# --- The hook and Examples 1 to 6, 13, and 14 ---------------------------------------------------


@pytest.mark.jvm_held  # the examples' formulas hand their globals, the sessions too, to the JVM
@pytest.mark.timeout(1200)
def test_the_free_examples_fill_on_stocks_basic(
    live_plans: plans.LivePlans,
    *,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    console_command: ConsoleCommand,
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    """Whether the hook and Examples 1 to 6, 13, and 14 fill on Stocks Basic, by a reader's path.

    Runs them back to back in one console with the main key on Stocks Basic and no setting, as
    a free reader does. Their sessions get a rate governor of their own, as in a new server,
    not the run's, which paces Basic from the first request: so Massive refuses the first
    request past 5 in a minute, and each session pauses and resumes inside the console's
    patience. The examples send about 15 stock requests, past the 5 a live test may send a
    class on Basic, so the test declares no Basic plan, and the run's pacing rule, which would
    skip it, leaves it alone. Skips unless the main key is on Stocks Basic.

    Asserts what `_run_free_examples` checks.
    """
    if live_plans.tiers["stocks"] != "basic":
        pytest.skip(_ON_BASIC)
    monkeypatch.setattr(rate_limits, "GOVERNORS", rate_limits.GovernorRegistry())
    evidence("Basic key with a governor of its own: Basic's limit is learned from Massive")
    run = _console(tmp_path, console_command, evidence)
    try:
        with doc_examples.recorded_sessions():
            _run_free_examples(run, table_rows=table_rows, evidence=evidence)
    finally:
        run.console.close()


@pytest.mark.needs("stocks:starter")
@pytest.mark.jvm_held  # the examples' formulas hand their globals, the sessions too, to the JVM
@pytest.mark.timeout(900)
def test_the_free_examples_fill_back_to_back_at_basics_pace(
    live_config: dm.MassiveConfig,
    *,
    tmp_path: Path,
    console_command: ConsoleCommand,
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    """Whether the hook and Examples 1 to 6, 13, and 14 fill with real data, at Basic's pace.

    A check of their content on a paid Stocks key. Runs them back to back in one console while
    a session that declares Stocks Basic (`rest_basic_subscriptions`) holds the key's rate
    governor, so every session's stock requests keep to Basic's 5 a minute. The setting paces
    from the first request, so this test never exercises a Basic reader's path, Massive's
    refusal of a sixth request and the pause after it;
    `test_the_free_examples_fill_on_stocks_basic` does, on a Basic key.

    Asserts what `_run_free_examples` checks.
    """
    evidence("paced as Basic by the setting: Massive's refusal of a sixth request is not exercised")
    stocks_basic = live_config.rest_basic_subscriptions | {dm.Subscription.STOCKS}
    pacer = dm.MassiveSession(
        dataclasses.replace(live_config, rest_basic_subscriptions=stocks_basic)
    )
    run = _console(tmp_path, console_command, evidence)
    try:
        with doc_examples.recorded_sessions():
            _run_free_examples(run, table_rows=table_rows, evidence=evidence)
    finally:
        run.console.close()
        pacer.close()


def _run_free_examples(run: _Run, *, table_rows: TableRows, evidence: Evidence) -> None:
    """Run the hook and Examples 1 to 6, 13, and 14 in one console, and check what Massive sent.

    Checks: the hook's year of Apple's bars; Example 1's five daily bars with their columns;
    Example 2's day of more than 1,000 stocks, none over the counter, and its top 20;
    Example 3's three returns; Example 4's year of Nvidia's bars; Example 5's headlines for
    each of its stocks, at most ten each, the limit Massive applies per ticker; Example 6's
    four previous closes, and the previous closes of the three Yahoo tickers that end it,
    under Massive's spellings; Example 13's Apple row, and its `ZZZZ` problem row quoting
    Massive's HTTP 404. Example 14 and Example 6's edit run as written; what they do is the
    library's, which `tests/test_docs.py` checks.
    """
    ran = {"hook": run.paste(doc_examples.intro_blocks(README), "hook", "apple")}
    keep = {
        "read-daily-bars": ("apple",),
        "whole-market-day": ("day", "busiest"),
        "compare-stocks": ("returns",),
        "chart-closes": ("nvda",),
    }
    for anchor, names in keep.items():
        ran[anchor] = run.section(anchor, *names)
    _check_history_examples(ran, table_rows=table_rows, evidence=evidence)
    news = run.section("follow-the-news", "headlines")
    _check_news(news["headlines"], table_rows=table_rows, evidence=evidence)
    opening, edit = doc_examples.section_blocks(README, "drive-with-a-watchlist")
    closes = run.paste([opening], "drive-with-a-watchlist", "closes")["closes"]
    first = {"AAPL", "MSFT", "AMZN", "GOOGL"}
    filled = _wait(lambda: _symbols(table_rows(closes)) == first, _FILL_WAIT)
    evidence(f"example 6: {sorted(_symbols(table_rows(closes)))}, filled: {filled}")
    assert filled, f"Example 6's closes did not fill within {_FILL_WAIT:.0f} s"
    run.paste([edit], "drive-with-a-watchlist edit")
    yahoo = run.section("convert-symbols", "yahoo_closes")["yahoo_closes"]
    converted = {"AAPL", "BRK.B", "BACpL"}
    filled = _wait(lambda: _symbols(table_rows(yahoo)) == converted, _FILL_WAIT)
    evidence(f"example 6, Yahoo tickers: {sorted(_symbols(table_rows(yahoo)))}, filled: {filled}")
    assert filled, f"the Yahoo tickers' closes did not fill within {_FILL_WAIT:.0f} s"
    problems = run.section("see-problems", "problems", "details")
    _check_problems(problems, table_rows=table_rows, evidence=evidence)
    run.section("stop-live-tables")


def _check_problems(ran: Names, *, table_rows: TableRows, evidence: Evidence) -> None:
    """Check Example 13: Apple's details, and `ZZZZ` as an `unavailable_symbols` row.

    Massive answers a ticker it doesn't list with HTTP 404 on ticker details, which the README
    states; the row's `Detail` quotes that answer.
    """

    def missing() -> list[dict[str, Any]]:
        return [
            row
            for row in table_rows(ran["problems"])
            if row["Cause"] == "unavailable_symbols" and row["Symbol"] == "ZZZZ"
        ]

    def apple() -> set[str]:
        return _symbols(table_rows(ran["details"]))

    found = _wait(lambda: bool(missing()) and apple() == {"AAPL"}, _FILL_WAIT)
    rows = missing()
    evidence(f"example 13: details {sorted(apple())}, problems {rows}, found: {found}")
    assert found, f"Example 13's rows did not appear within {_FILL_WAIT:.0f} s"
    [row] = rows
    assert row["Action"] == "SKIPPED"
    assert "HTTP 404" in row["Detail"]


def _check_history_examples(
    ran: dict[str, Names], *, table_rows: TableRows, evidence: Evidence
) -> None:
    """Check the hook and Examples 1 to 4: real bars, in plausible numbers.

    Each reads in the background, so each check first waits for its table to fill.
    """

    def when_filled(table: Table, enough: Callable[[int], bool]) -> list[dict[str, Any]]:
        _wait(lambda: enough(len(table_rows(table))), _FILL_WAIT)
        return table_rows(table)

    year = when_filled(ran["hook"]["apple"], lambda rows: rows >= 240)
    evidence(f"hook: {len(year)} daily bars of AAPL")
    assert len(year) >= 240, "the hook's year of Apple's daily bars is short"

    first = ran["read-daily-bars"]["apple"]
    week = when_filled(first, lambda rows: rows == 5)
    evidence(f"example 1: {len(week)} bars, dates {[row['Date'] for row in week]}")
    assert len(week) == 5, "Example 1 reads five trading days"
    assert set(_BAR_COLUMNS) <= set(first.column_names)

    market = ran["whole-market-day"]
    day = when_filled(market["day"], lambda rows: rows > 1000)
    otc = [row["Symbol"] for row in day if row.get("Otc")]
    evidence(f"example 2: {len(day)} stocks, {len(otc)} over the counter")
    assert len(day) > 1000, "Example 2's day holds every listed US stock"
    assert not otc, f"Example 2's day holds stocks over the counter: {otc[:5]}"
    assert len(when_filled(market["busiest"], lambda rows: rows == 20)) == 20

    compared = ran["compare-stocks"]["returns"]
    filled = _wait(lambda: _symbols(table_rows(compared)) == _STOCKS, _FILL_WAIT)
    returns = table_rows(compared)
    evidence(f"example 3: {[(r['Symbol'], r['ReturnPct']) for r in returns]}, filled: {filled}")
    assert filled, f"Example 3's returns did not fill within {_FILL_WAIT:.0f} s"

    nvda = when_filled(ran["chart-closes"]["nvda"], lambda rows: rows >= 240)
    evidence(f"example 4: {len(nvda)} daily bars of NVDA")
    assert len(nvda) >= 240, "Example 4's year of Nvidia's daily bars is short"


def _check_news(headlines: Table, *, table_rows: TableRows, evidence: Evidence) -> None:
    """Check Example 5: headlines for each stock, at most ten each."""
    asked = {"AAPL", "NVDA", "TSLA"}
    filled = _wait(lambda: _symbols(table_rows(headlines)) == asked, _FILL_WAIT)
    rows = table_rows(headlines)
    counts = {symbol: sum(row["Symbol"] == symbol for row in rows) for symbol in sorted(asked)}
    evidence(f"example 5: articles per stock {counts}, every stock in by the wait: {filled}")
    assert filled, f"Example 5's headlines hold {counts} after {_FILL_WAIT:.0f} s"
    assert all(count <= 10 for count in counts.values()), f"more than ten for a stock: {counts}"


# --- Examples 7 to 12, on a Stocks plan that streams --------------------------------------------


@pytest.mark.needs("stocks:starter")
@pytest.mark.jvm_held  # the examples' formulas hand their globals, the sessions too, to the JVM
@pytest.mark.timeout(600)
def test_the_paid_examples_fill_in_the_current_stock_session(
    live_plans: plans.LivePlans,
    live_session: dm.MassiveSession,
    *,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    console_command: ConsoleCommand,
    dh_exec_ctx: ExecutionContext,
    wait_acked: WaitAcked,
    table_rows: TableRows,
    stream_report: StreamReport,
    evidence: Evidence,
) -> None:
    """Whether Examples 7 to 12 fill with what the stock session of the moment sends.

    Saves the settings file with the README's commands for the key's Stocks plan (delayed on
    Starter and Developer), in a stand-in home folder, runs Examples 7 to 12 in one console,
    in order, and collects whatever the console no longer reaches, such as the sessions each
    later example's `m` replaced. Then watches the streams across three minute boundaries.

    Asserts at any hour: Example 7's snapshot of three stocks with its columns, except while
    Massive's snapshots are empty (`doc_examples.snapshots_cleared`); Example 12's three topics
    acknowledged and none of its streams failed. Then, by the session the plan's feed is in
    (`streams.stock_session`):

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
    the checks that hold at any hour.
    """
    monkeypatch.setenv("HOME", str(tmp_path))
    delayed = not plans.unmet(("stocks:delayed",), live_plans)
    doc_examples.write_settings_file(README, tmp_path, delayed=delayed)
    delay = streams.FEED_DELAYS.get(("stocks", live_plans.tiers["stocks"]), timedelta(0))
    evidence(f"settings file for a {'delayed' if delayed else 'real-time'} plan; delay {delay}")
    run = _console(tmp_path, console_command, evidence)

    def locked_rows(*tables: Table) -> list[Rows]:
        with update_graph.shared_lock(dh_exec_ctx.update_graph):
            return [table_rows(table) for table in tables]

    try:
        with doc_examples.recorded_sessions():
            snapshot = run.section("what-your-plan-allows", "snapshot")["snapshot"]
            _check_snapshot(snapshot, delay, table_rows=table_rows, evidence=evidence)
            first_session = streams.stock_session(datetime.now(UTC), delay)
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
            assert acked >= len(_STOCKS), "Example 12's topics were not all acknowledged"
            seen = [
                _watch_minute(
                    ran, locked_rows=locked_rows, table_rows=table_rows, evidence=evidence
                )
                for _ in range(_MINUTES_WATCHED)
            ]
            failed = [r["Error"] for r in table_rows(board.streams()) if r["State"] == "FAILED"]
            assert not failed, f"Example 12's stream failed: {failed}"
            last_session = streams.stock_session(datetime.now(UTC), delay)
            evidence(f"stock session of the feed: {first_session}, then {last_session}")
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
    snapshot: Table, delay: timedelta, *, table_rows: TableRows, evidence: Evidence
) -> None:
    """Check Example 7: a snapshot of three stocks, with the columns its text names."""
    assert {"DayClose", "PrevDayClose", "TodaysChangePerc"} <= set(snapshot.column_names)
    _wait(lambda: bool(table_rows(snapshot)), _FILL_WAIT)  # it reads in the background
    rows = table_rows(snapshot)
    evidence(f"example 7: {[(r['Symbol'], r['DayClose']) for r in rows]}")
    if doc_examples.snapshots_cleared(datetime.now(UTC), delay):
        evidence("example 7's rows unchecked: Massive's snapshots are empty from 03:30 to 04:00")
        return
    assert _symbols(rows) == _STOCKS


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
        Each table's rows, by name, and `published`, the rows the dashboard's stream published.
    """
    _sleep_until(streams.minute_bar_deadline(datetime.now(UTC)))
    names = ("latest", "changes", "since_start", "spy")
    tables = (
        ran["stream-minute-bars"]["latest"],
        ran["change-since-close"]["changes"],
        ran["table-shapes"]["since_start"],
        ran["backfill-then-live"]["spy"],
    )
    seen: dict[str, Any] = dict(zip(names, locked_rows(*tables), strict=True))
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


# --- Single examples on other plans ------------------------------------------------------------


@pytest.mark.needs("stocks:starter")
@pytest.mark.timeout(200)
def test_the_settings_example_without_its_setting_is_refused_on_a_delayed_plan(
    delayed_key: str,
    *,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    console_command: ConsoleCommand,
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    """Whether Example 7's snapshot fails on a delayed plan without its setting.

    Saves the real-time plans' settings file, `delayed_subscriptions: []`, with the README's
    own commands, then runs Example 7 with a delayed Stocks key (`MASSIVE_DELAYED_API_KEY`, or
    the main key on Starter or Developer). The snapshot reads in the background, so the refusal
    fails its table. Passes when the read's row in `m.streams()` is `FAILED` and its `Error`
    says stocks are not in `delayed_subscriptions`, which sets this refusal apart from one of a
    plan without snapshots, such as a Basic key's.
    """
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("MASSIVE_API_KEY", delayed_key)
    doc_examples.write_settings_file(README, tmp_path, delayed=False)
    [block] = doc_examples.section_blocks(README, "what-your-plan-allows")
    script = tmp_path / "settings_example.py"
    script.write_text(block.code, encoding="utf-8")
    ran: list[dict[str, Any]] = []

    def example() -> None:
        ran.append(runpy.run_path(str(script)))

    def errors() -> list[str]:
        streams = ran[0]["m"].streams()
        return [str(r["Error"]) for r in table_rows(streams) if r["State"] == "FAILED"]

    with doc_examples.recorded_sessions():
        console_command(example, timeout=_CONSOLE_WAIT)
        _wait(lambda: bool(errors()), _FILL_WAIT)
        failed = errors()
        ran[0]["m"].close()
    evidence(f"example 7 without the setting: {failed or 'no failure'}")
    assert failed, "Example 7 read the snapshot without its setting"
    assert _DELAYED_REFUSAL in failed[0], "the refusal does not name delayed_subscriptions"


@pytest.mark.needs("currencies:starter")
@pytest.mark.timeout(150)
def test_the_streaming_examples_crypto_line_streams_now(
    *,
    tmp_path: Path,
    console_command: ConsoleCommand,
    wait_acked: WaitAcked,
    table_rows: TableRows,
    stream_report: StreamReport,
    evidence: Evidence,
) -> None:
    """Whether the streaming example's crypto line fills on Currencies Starter, at any hour.

    Runs the line as the example's text gives it, in a session opened as the README's examples
    open theirs, and waits for the first minute bar past the next minute boundary. Passes when
    Massive acknowledged the topic and a bar arrived.
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
"""The starts of the three lines the tester guide's example 10 prints on Basic, as it says."""

_REFUSED_TOPIC: Final = "AM.AAPL"
_REJECTED_KEY: Final = "not only a bad key"
"""How a failed stream's error quotes Massive's refusal of the topic, and how it says Massive
rejected the key, the guide's two outcomes."""


@pytest.mark.needs("stocks:basic")
@pytest.mark.requests(2)
@pytest.mark.timeout(240)
def test_tester_example_10_fails_the_live_minute_bars_on_basic(
    live_plans: plans.LivePlans,
    *,
    tmp_path: Path,
    console_command: ConsoleCommand,
    table_rows: TableRows,
    capsys: pytest.CaptureFixture[str],
    evidence: Evidence,
) -> None:
    """Whether the tester guide's example 10 prints its three lines and fails its live bars.

    Runs the example as the guide gives it with the main key on Stocks Basic, which no plan
    setting of a paid key can stand in for; skips on any other Stocks plan. It sends the key's
    stocks two requests, each of which may wait out Basic's pace inside the console's
    patience. Passes when it prints the guide's three lines (the latest trade and the bars
    from 2015 refused with `EntitlementError`, the live bars opened), and the `stocks.bars`
    row of `streams` reads `FAILED` with one of the guide's two outcomes: Massive's refusal of
    `AM.AAPL`, or Massive's rejection of the key.
    """
    if live_plans.tiers["stocks"] != "basic":
        pytest.skip(_ON_BASIC)
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
            for row in table_rows(names["connections"]):
                evidence(f"connection {row['AssetClass']} {row['State']} {row['LastError']!r}")
            names.clear()
    finally:
        run.console.close()
    evidence(f"stocks.bars: {[(row['State'], row['Error']) for row in rows]}")
    for start in _TESTER_LINES:
        assert any(line.startswith(start) for line in printed), f"no line starts {start!r}"
    assert [row["State"] for row in rows] == ["FAILED"]
    error = str(rows[0]["Error"])
    assert _REFUSED_TOPIC in error or _REJECTED_KEY in error, error
