"""Live runs of the user docs' examples, each as written, checked against what its text says.

Every example is read from its page when the test runs (`tests/_live/doc_examples.py`), so the
code that runs is the code a reader pastes, and it runs as the Deephaven console runs it: one
command per block, under the update graph's exclusive lock. The tests follow the README's own
flow, one console per group of examples:

- the hook and Examples 1 to 7, back to back at Stocks Basic's pace of 5 requests a minute;
- Examples 8 to 13, which load the settings file Example 8 saves, checked against what the
  stock session of the moment sends: pre-market, regular, after hours, or overnight;
- Example 8 without its setting, on a delayed Stocks plan;
- the streaming example's crypto line, on Currencies Starter;
- the tester guide's example 10, on a Stocks Basic key.

A claim that needs another stock session than the current one goes unchecked, and the evidence
names the session. Each test records what it saw (`evidence`). How to run these tests is in
`docs/live-tests.md`.
"""

from __future__ import annotations

import dataclasses
import functools
import itertools
import math
import re
import runpy
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final

import pytest
from deephaven import table_listener, update_graph
from deephaven.plot.express.deephaven_figure.DeephavenFigure import DeephavenFigure
from deephaven.table import Table

import deephaven_massive as dm
from tests._live import doc_examples, plans, streams
from tests._live.calendar import NEW_YORK, is_trading_day
from tests._live.tester.examples import guide_blocks

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Sequence
    from typing import TypeAlias

    from deephaven.execution_context import ExecutionContext

pytestmark = pytest.mark.integration

# Fixture types, repeated in each live module: test modules may not import from conftest.
TableRows: TypeAlias = "Callable[[Table], list[dict[str, Any]]]"
Evidence: TypeAlias = "Callable[[str], None]"
StreamReport: TypeAlias = "Callable[[dm.MassiveSession], list[str]]"
WaitAcked: TypeAlias = "Callable[[dm.MassiveSession, int, float], int]"
ConsoleCommand: TypeAlias = "Callable[..., None]"
Namespace: TypeAlias = "dict[str, Any]"
Rows: TypeAlias = "list[dict[str, Any]]"

_ROOT = Path(__file__).resolve().parent.parent.parent
README = _ROOT / "README.md"
TESTER_GUIDE = _ROOT / "docs" / "tester-guide.md"

FREE_SECTIONS: Final = (
    "read-daily-bars",
    "whole-market-day",
    "compare-stocks",
    "chart-closes",
    "follow-the-news",
    "stop-live-tables",
    "drive-with-a-watchlist",
)
"""The README's Stocks Basic examples, 1 to 7, which follow its hook; run in this order."""

PAID_SECTIONS: Final = (
    "what-your-plan-allows",
    "stream-minute-bars",
    "change-since-close",
    "table-shapes",
    "backfill-then-live",
    "dashboard",
)
"""The README's Stocks Starter examples, 8 to 13; run in this order."""

UNRUN_SECTIONS: Final = {
    "massive-config": "it opens a session and reads nothing; tests/test_docs.py runs it",
}
"""The README sections with Python blocks that no live test runs, and why."""

_CONSOLE_WAIT: Final = 120.0
"""Seconds one console command may take: a read paced at Basic's rate waits up to 90 s under
the console's lock before it gives up."""

_FILL_WAIT: Final = 90.0
"""Seconds a poll's first rows may take, a paced request's wait for its minute included."""

_NEXT_POLL_WAIT: Final = 150.0
"""Seconds a once-a-minute poll may take to show a watchlist edit: the minute, then the wait of
its paced requests."""

_CLOSE_WAIT: Final = 30.0
"""Seconds a closed session's tables may take to fail."""

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
"""The stocks Examples 8, 9, 10, and 13 read."""


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
    """Return the `Symbol` of each row."""
    return {str(row["Symbol"]) for row in rows}


@dataclasses.dataclass
class _Console:
    """One console the examples run in, one command per block, and the names they define.

    Attributes:
        tmp_path: Where the blocks' scripts are written.
        command: Runs one block as the console runs a command (`console_command`).
        evidence: Records when each example ran.
        namespaces: The names each block defined, in order, for `doc_examples.forget`.
        started: When the console opened, by `time.monotonic()`.
    """

    tmp_path: Path
    command: ConsoleCommand
    evidence: Evidence
    namespaces: list[Namespace] = dataclasses.field(default_factory=list)
    started: float = dataclasses.field(default_factory=time.monotonic)

    def run(
        self, blocks: Sequence[doc_examples.Block], name: str, *, start: Namespace | None = None
    ) -> Namespace:
        """Run blocks in order, and return the names the last one defined.

        Args:
            blocks: The blocks.
            name: The example, for the evidence and the scripts' names.
            start: The names a first block that continues an earlier example starts from.

        Returns:
            The last block's names.
        """
        file_name = re.sub(r"\W", "_", name)
        ran = doc_examples.run_blocks(
            blocks, self.tmp_path, file_name, console_command=self.command, start=start
        )
        self.namespaces.extend(ran)
        self.evidence(f"{name}: ran by {time.monotonic() - self.started:.0f} s")
        return ran[-1]

    def section(self, anchor: str, *, start: Namespace | None = None) -> Namespace:
        """Run the Python blocks of one README section, and return the last one's names.

        Args:
            anchor: The section's `<a id>`.
            start: The names a first block that continues an earlier example starts from.

        Returns:
            The last block's names.
        """
        return self.run(doc_examples.section_blocks(README, anchor), anchor, start=start)


class _Failure:
    """The text of the failure a table's listener heard, written on an update-graph thread."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._text: str | None = None

    def heard(self, error: Exception) -> None:
        """Keep the failure's text."""
        with self._lock:
            self._text = str(error)

    def text(self) -> str | None:
        """Return the failure's text, or None before the table fails."""
        with self._lock:
            return self._text


@pytest.fixture
def failure_text() -> Iterator[Callable[[Table], _Failure]]:
    """Return a function that listens for a live table's failure and keeps its text.

    Every listener is stopped when the test ends, before its leak check.

    Yields:
        A function mapping a table to the record of its failure.
    """
    handles = []

    def watch(table: Table) -> _Failure:
        failure = _Failure()
        # A wrapper of its own: the JVM holds the listener, and through it the wrapper.
        own = Table(j_table=table.j_table)
        handles.append(
            table_listener.listen(own, lambda _update, _replay: None, on_error=failure.heard)
        )
        return failure

    yield watch
    for handle in handles:
        handle.stop()


# --- The hook and Examples 1 to 7, on Basic's pace ---------------------------------------------


@pytest.mark.needs("stocks:starter")
@pytest.mark.jvm_held  # the examples' formulas hand their globals, the sessions too, to the JVM
@pytest.mark.timeout(900)
def test_the_free_examples_fill_back_to_back_at_basics_pace(
    live_config: dm.MassiveConfig,
    *,
    tmp_path: Path,
    console_command: ConsoleCommand,
    failure_text: Callable[[Table], _Failure],
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    """Whether the hook and Examples 1 to 7 fill as their text says, run back to back on Basic.

    Runs the README's hook and its Stocks Basic examples in one console, in order, Example 6
    continuing Example 5 and Example 7's edit continuing Example 7, while a session that
    declares Stocks Basic (`rest_basic_subscriptions`) holds the key's rate governor, so every
    session's stock requests keep to Basic's 5 a minute: the examples send about 15 in their
    first minutes. A paid Stocks plan stands in for Basic, whose own pacing of a whole live
    run would make this one test take a quarter hour; Massive's refusal of a sixth request,
    which the setting avoids, is the rate-limit tier's to check.

    Asserts: the hook's year of Apple's bars and its chart; Example 1's five daily bars with
    their columns; Example 2's whole-market day without OTC stocks and its top 20 by dollar
    volume; Example 3's three returns; Example 4's year of Nvidia's bars and its chart;
    Example 5's headlines, up to ten per stock and newest first; Example 6's closed-session
    error on `headlines` and `latest`, their rows kept, and `kept` the same rows; Example 7's
    four previous closes, then Nvidia's row in and Microsoft's out at the next poll, the other
    rows unchanged.
    """
    stocks_basic = live_config.rest_basic_subscriptions | {dm.Subscription.STOCKS}
    pacer = dm.MassiveSession(
        dataclasses.replace(live_config, rest_basic_subscriptions=stocks_basic)
    )
    console = _Console(
        tmp_path, functools.partial(console_command, timeout=_CONSOLE_WAIT), evidence
    )
    try:
        with doc_examples.recorded_sessions():
            ran = {"hook": console.run(doc_examples.intro_blocks(README), "hook")}
            for anchor in FREE_SECTIONS[:5]:
                ran[anchor] = console.section(anchor)
            _check_history_examples(ran, table_rows=table_rows, evidence=evidence)
            news = ran["follow-the-news"]
            _check_news(news, table_rows=table_rows, evidence=evidence)
            failures = {name: failure_text(news[name]) for name in ("headlines", "latest")}
            before_close = table_rows(news["latest"])
            stopped = console.section("stop-live-tables", start=news)
            _check_stopped(
                stopped, before_close, failures=failures, table_rows=table_rows, evidence=evidence
            )
            _check_watchlist(console, table_rows=table_rows, evidence=evidence)
    finally:
        doc_examples.forget(console.namespaces)
        pacer.close()


def _check_history_examples(
    ran: dict[str, Namespace], *, table_rows: TableRows, evidence: Evidence
) -> None:
    """Check the hook and Examples 1 to 4, which read history once."""
    hook = ran["hook"]
    year = table_rows(hook["apple"])
    evidence(f"hook: {len(year)} daily bars of AAPL, chart {type(hook['chart']).__name__}")
    assert len(year) >= 240, "the hook's year of Apple's daily bars is short"
    assert isinstance(hook["chart"], DeephavenFigure)

    first = ran["read-daily-bars"]
    week = table_rows(first["apple"])
    evidence(f"example 1: {len(week)} bars, dates {[row['Date'] for row in week]}")
    assert len(week) == 5, "Example 1 reads five trading days"
    assert set(_BAR_COLUMNS) <= set(first["apple"].column_names)

    market = ran["whole-market-day"]
    day = table_rows(market["day"])
    busiest = table_rows(market["busiest"])
    otc = [row["Symbol"] for row in day if row.get("Otc")]
    volumes = [row["DollarVolume"] for row in busiest]
    evidence(f"example 2: {len(day)} stocks, {len(otc)} OTC, top {[r['Symbol'] for r in busiest]}")
    assert len(day) > 1000, "Example 2's day holds every listed US stock"
    assert not otc, f"Example 2 leaves OTC stocks out, but has {otc[:5]}"
    assert len(busiest) == 20
    assert volumes == sorted(volumes, reverse=True)

    returns = table_rows(ran["compare-stocks"]["returns"])
    evidence(f"example 3: {[(r['Symbol'], r['ReturnPct']) for r in returns]}")
    assert _symbols(returns) == _STOCKS
    assert all(row["ReturnPct"] is not None for row in returns)

    charted = ran["chart-closes"]
    nvda = table_rows(charted["nvda"])
    evidence(f"example 4: {len(nvda)} daily bars of NVDA, chart {type(charted['closes']).__name__}")
    assert len(nvda) >= 240, "Example 4's year of Nvidia's daily bars is short"
    assert isinstance(charted["closes"], DeephavenFigure)


def _check_news(news: Namespace, *, table_rows: TableRows, evidence: Evidence) -> None:
    """Check Example 5: up to ten articles per stock, newest first."""
    asked = {"AAPL", "NVDA", "TSLA"}
    filled = _wait(lambda: _symbols(table_rows(news["headlines"])) == asked, _FILL_WAIT)
    rows = table_rows(news["latest"])
    counts = {symbol: sum(row["Symbol"] == symbol for row in rows) for symbol in sorted(asked)}
    published = [row["PublishedUtc"] for row in rows]
    evidence(f"example 5: articles per stock {counts}, every stock in by the wait: {filled}")
    assert filled, f"Example 5's headlines hold {counts} after {_FILL_WAIT:.0f} s"
    assert all(count <= 10 for count in counts.values()), f"more than ten for a stock: {counts}"
    assert published == sorted(published, reverse=True), "Example 5's latest is not newest first"


def _check_stopped(
    stopped: Namespace,
    before_close: Rows,
    *,
    failures: dict[str, _Failure],
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    """Check Example 6: the closed-session error, the rows kept, and `kept` the same."""
    failed = _wait(lambda: all(f.text() is not None for f in failures.values()), _CLOSE_WAIT)
    texts = {name: failure.text() for name, failure in failures.items()}
    kept = table_rows(stopped["kept"])
    latest = table_rows(stopped["latest"])
    evidence(f"example 6: errors {texts}; latest {len(latest)} rows, kept {len(kept)} rows")
    assert failed, f"a closed session's tables did not fail within {_CLOSE_WAIT:.0f} s: {texts}"
    for name, text in texts.items():
        assert "was closed" in str(text), f"{name}'s error does not say the session was closed"
    assert latest == before_close, "latest lost or changed rows when the session closed"
    assert kept == latest, "kept does not hold latest's rows"
    assert kept, "Example 6 kept no rows"


def _check_watchlist(console: _Console, *, table_rows: TableRows, evidence: Evidence) -> None:
    """Run and check Example 7: four previous closes, then Nvidia in and Microsoft out."""
    opening, edit = doc_examples.section_blocks(README, "drive-with-a-watchlist")
    followed = console.run([opening], "drive-with-a-watchlist")
    closes = followed["closes"]
    filled = _wait(
        lambda: _symbols(table_rows(closes)) == {"AAPL", "MSFT", "AMZN", "GOOGL"}, _FILL_WAIT
    )
    before = {row["Symbol"]: row for row in table_rows(closes)}
    evidence(f"example 7: {sorted(before)}, filled: {filled}")
    assert filled, f"Example 7's closes hold {sorted(before)} after {_FILL_WAIT:.0f} s"
    edited = time.monotonic()
    console.run([edit], "drive-with-a-watchlist edit", start=followed)
    changed = _wait(
        lambda: _symbols(table_rows(closes)) == {"AAPL", "AMZN", "GOOGL", "NVDA"}, _NEXT_POLL_WAIT
    )
    after = {row["Symbol"]: row for row in table_rows(closes)}
    evidence(
        f"example 7: after the edit {sorted(after)} in {time.monotonic() - edited:.0f} s; "
        f"changed: {changed}"
    )
    assert changed, f"Example 7's closes hold {sorted(after)} after the edit"
    for symbol in ("AAPL", "AMZN", "GOOGL"):
        kept = [after[symbol][column] for column in _BAR_COLUMNS]
        assert kept == [before[symbol][column] for column in _BAR_COLUMNS], symbol


# --- Examples 8 to 13, on a Stocks plan that streams --------------------------------------------


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
    """Whether Examples 8 to 13 fill as their text says in the stock session of the moment.

    Saves the settings file with the README's commands for the key's Stocks plan (delayed on
    Starter and Developer), in a stand-in home folder, and runs Examples 8 to 13 in one
    console, in order. Then watches the streams across three minute boundaries.

    Asserts at any hour: Example 8's snapshot of three stocks with its columns; every stream
    acknowledged and none failed; Example 10's rows each with a `PrevClose`, and a `Close` only
    for a stock with a bar; Example 13's dashboard with the chart and the change table in its
    two panels. Then, by the session the plan's feed is in (`streams.stock_session`):

    - pre-market, regular, or after hours: Example 9's minute bars; Example 10's `Close` and
      `ChangePct` from them; Example 11's three shapes of SPY's bars; Example 12's `spy` with
      every minute REST has of the day so far, none twice, across the seam with the stream;
      the dashboard's stream growing. In the regular session, every stock has its bar, Example
      9's rows change each minute, and the dashboard's stream grows each minute.
    - overnight: Example 9's `latest` and the other live tables stay empty, and `spy` holds
      the day's bars only from history, or none on a day the market is closed.

    A run whose feed crosses from one session to another while it watches is skipped after
    the checks that hold at any hour.
    """
    monkeypatch.setenv("HOME", str(tmp_path))
    delayed = not plans.unmet(("stocks:delayed",), live_plans)
    doc_examples.write_settings_file(README, tmp_path, delayed=delayed)
    delay = streams.FEED_DELAYS.get(("stocks", live_plans.tiers["stocks"]), timedelta(0))
    evidence(f"settings file for a {'delayed' if delayed else 'real-time'} plan; delay {delay}")
    console = _Console(
        tmp_path, functools.partial(console_command, timeout=_CONSOLE_WAIT), evidence
    )

    def locked_rows(*tables: Table) -> list[Rows]:
        with update_graph.shared_lock(dh_exec_ctx.update_graph):
            return [table_rows(table) for table in tables]

    try:
        with doc_examples.recorded_sessions() as opened:
            ran = {"what-your-plan-allows": console.section("what-your-plan-allows")}
            _check_snapshot(ran["what-your-plan-allows"], table_rows=table_rows, evidence=evidence)
            first_session = streams.stock_session(datetime.now(UTC), delay)
            for anchor in ("stream-minute-bars", "change-since-close"):
                ran[anchor] = console.section(anchor)
            change = ran["change-since-close"]
            _check_before_bars(*locked_rows(change["changes"], change["latest"]), evidence=evidence)
            for anchor in ("table-shapes", "backfill-then-live", "dashboard"):
                ran[anchor] = console.section(anchor)
            _check_dashboard(ran["dashboard"], evidence=evidence)
            for session in opened[1:]:
                acked = wait_acked(session, 1, time.monotonic() + _ACK_WAIT)
                for line in stream_report(session):
                    evidence(line)
                assert acked, "a streaming example's topics were not acknowledged"
            seen = [
                _watch_minute(
                    ran, locked_rows=locked_rows, table_rows=table_rows, evidence=evidence
                )
                for _ in range(_MINUTES_WATCHED)
            ]
            failed = [
                row["Error"]
                for session in opened[1:]
                for row in table_rows(session.streams())
                if row["State"] == "FAILED"
            ]
            assert not failed, f"a streaming example's stream failed: {failed}"
            last_session = streams.stock_session(datetime.now(UTC), delay)
            evidence(f"stock session of the feed: {first_session}, then {last_session}")
            if first_session != last_session:
                pytest.skip(f"the feed moved from {first_session} to {last_session} meanwhile")
            if first_session == streams.OVERNIGHT:
                _check_overnight(seen[-1], live_session, table_rows=table_rows, evidence=evidence)
            else:
                _check_streaming(seen, first_session, evidence=evidence)
                _check_seam(
                    ran["backfill-then-live"],
                    live_session,
                    table_rows=table_rows,
                    evidence=evidence,
                )
    finally:
        doc_examples.forget(console.namespaces)


def _check_snapshot(snapshot: Namespace, *, table_rows: TableRows, evidence: Evidence) -> None:
    """Check Example 8: a snapshot of three stocks, with the columns its text names."""
    rows = table_rows(snapshot["snapshot"])
    evidence(f"example 8: {[(r['Symbol'], r['DayClose']) for r in rows]}")
    assert _symbols(rows) == _STOCKS
    assert {"DayClose", "PrevDayClose", "TodaysChangePerc"} <= set(
        snapshot["snapshot"].column_names
    )


def _check_before_bars(changes: Rows, latest: Rows, *, evidence: Evidence) -> None:
    """Check Example 10 as it starts: a `PrevClose` for each stock, a `Close` only with a bar."""
    barred = _symbols(latest)
    evidence(f"example 10 at the start: {changes}; stocks with a bar {sorted(barred)}")
    assert _symbols(changes) == _STOCKS
    for row in changes:
        assert row["PrevClose"] is not None, row
        assert (row["Close"] is not None) == (row["Symbol"] in barred), row


def _check_dashboard(board: Namespace, *, evidence: Evidence) -> None:
    """Check Example 13's dashboard: the chart and the change table, each in its panel."""
    panels = doc_examples.dashboard_panels(board["dashboard"])
    evidence(f"example 13: panels {[(title, type(content).__name__) for title, content in panels]}")
    assert [title for title, _ in panels] == ["Closes", "Change"]
    assert panels[0][1] is board["chart"]
    assert isinstance(board["chart"], DeephavenFigure)
    assert panels[1][1] is board["changes"]


def _watch_minute(
    ran: dict[str, Namespace],
    *,
    locked_rows: Callable[..., list[Rows]],
    table_rows: TableRows,
    evidence: Evidence,
) -> dict[str, Any]:
    """Wait past the next minute boundary, then read every streaming example's tables.

    Args:
        ran: The names of each example, by its README anchor.
        locked_rows: Reads tables together, under the update graph's shared lock.
        table_rows: Reads one table.
        evidence: Records the counts.

    Returns:
        Each table's rows, by name, and `published`, the rows the dashboard's stream published.
    """
    _sleep_until(streams.minute_bar_deadline(datetime.now(UTC)))
    shapes = ran["table-shapes"]
    names = ("latest", "changes", "newest", "last_30", "since_start", "spy")
    tables = (
        ran["stream-minute-bars"]["latest"],
        ran["change-since-close"]["changes"],
        shapes["newest"],
        shapes["last_30"],
        shapes["since_start"],
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
    """Check Examples 9 to 13 while the stock market trades."""
    last = seen[-1]
    regular = session == streams.REGULAR
    evidence(f"checked in the {session} session")
    assert last["latest"], f"no minute bar of Example 9 arrived in the {session} session"
    assert _symbols(last["latest"]) <= _STOCKS
    for row in last["changes"]:
        if row["Close"] is not None:
            expected = 100 * (row["Close"] / row["PrevClose"] - 1)
            assert math.isclose(row["ChangePct"], expected, rel_tol=1e-9), row
    assert last["since_start"], "Example 11's since_start holds no bar"
    assert len(last["newest"]) == 1
    assert len(last["last_30"]) == min(30, len(last["since_start"]))
    published = [minute["published"] for minute in seen]
    assert published[-1] > 0, "Example 13's chart got no bar"
    if not regular:
        return
    assert _symbols(last["latest"]) == _STOCKS
    ends = {row["Symbol"]: row["TimestampEnd"] for row in seen[0]["latest"]}
    moved = [r["Symbol"] for r in last["latest"] if r["TimestampEnd"] != ends.get(r["Symbol"])]
    assert moved, "no row of Example 9's latest changed as the minutes ended"
    assert all(row["Close"] is not None for row in last["changes"]), last["changes"]
    grew = all(later > earlier for earlier, later in itertools.pairwise(published))
    assert grew, f"Example 13's chart did not grow each minute: {published}"


def _check_seam(
    backfill: Namespace,
    live_session: dm.MassiveSession,
    *,
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    """Check Example 12: every minute REST has of the day so far, none twice, across the seam."""
    spy = [row["TimestampStart"] for row in table_rows(backfill["spy"])]
    live = [row["TimestampStart"] for row in table_rows(backfill["live"])]
    assert len(spy) == len(set(spy)), "Example 12's spy holds a minute twice"
    assert live, "no live bar reached Example 12"
    rest = _rest_minutes(live_session, table_rows)
    rest = {start for start in rest if start <= max(live)}
    held = {start for start in spy if start <= max(live)}
    missing = sorted(rest - held)
    evidence(
        f"example 12: {len(held)} minutes through the last live bar, {len(live)} live; "
        f"REST has {len(rest)}; missing {missing[:5]} ({len(missing)}); "
        f"only in spy {sorted(held - rest)[:5]}"
    )
    assert not missing, f"Example 12's spy lacks {len(missing)} minutes that REST has"


def _rest_minutes(live_session: dm.MassiveSession, table_rows: TableRows) -> set[int]:
    """Return the start of every minute bar REST has of SPY today, in New York, as epoch ns."""
    today = datetime.now(NEW_YORK).date().isoformat()
    history = live_session.stocks.bars("SPY", dm.Timespan.MINUTE).history(today)
    return {int(row["TimestampStart"]) for row in table_rows(history)}


def _check_overnight(
    last: dict[str, Any],
    live_session: dm.MassiveSession,
    *,
    table_rows: TableRows,
    evidence: Evidence,
) -> None:
    """Check Examples 9 to 12 between sessions: the live tables empty, `spy` only history."""
    evidence("checked overnight")
    assert not last["latest"], "Example 9's latest filled overnight"
    assert not last["since_start"], "Example 11's since_start filled overnight"
    assert all(row["Close"] is None for row in last["changes"]), last["changes"]
    spy = {row["TimestampStart"] for row in last["spy"]}
    if not is_trading_day(datetime.now(NEW_YORK).date()):
        evidence(f"example 12 on a closed day: {len(spy)} rows")
        assert not spy, "Example 12's spy holds bars on a day the market is closed"
        return
    rest = _rest_minutes(live_session, table_rows)
    evidence(f"example 12 overnight: spy {len(spy)} minutes, REST {len(rest)}")
    assert spy == rest, "Example 12's spy does not hold the day's minutes from history"


# --- Single examples on other plans ------------------------------------------------------------


@pytest.mark.needs("stocks:starter")
@pytest.mark.timeout(100)
def test_the_settings_example_without_its_setting_is_refused_on_a_delayed_plan(
    delayed_key: str,
    *,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    console_command: ConsoleCommand,
    evidence: Evidence,
) -> None:
    """Whether Example 8 raises `dm.EntitlementError` on a delayed plan without its setting.

    Saves the real-time plans' settings file, `delayed_subscriptions: []`, with the README's
    own commands, then runs Example 8 with a delayed Stocks key (`MASSIVE_DELAYED_API_KEY`, or
    the main key on Starter or Developer). Passes when the snapshot raises
    `dm.EntitlementError`; the evidence quotes its message.
    """
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("MASSIVE_API_KEY", delayed_key)
    doc_examples.write_settings_file(README, tmp_path, delayed=False)
    [block] = doc_examples.section_blocks(README, "what-your-plan-allows")
    script = tmp_path / "settings_example.py"
    script.write_text(block.code, encoding="utf-8")
    raised: list[tuple[str, str]] = []

    def example() -> None:
        try:
            runpy.run_path(str(script))
        except dm.MassiveError as error:
            raised.append((type(error).__name__, str(error)))

    with doc_examples.recorded_sessions():
        console_command(example)
    evidence(f"example 8 without the setting: {raised or 'no error'}")
    assert raised, "Example 8 read the snapshot without its setting"
    assert raised[0][0] == "EntitlementError", raised[0]


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
    console = _Console(tmp_path, console_command, evidence)
    try:
        with doc_examples.recorded_sessions() as opened:
            names = console.run([doc_examples.Block(code, continues=False)], "crypto line")
            acked = wait_acked(opened[0], 1, time.monotonic() + _ACK_WAIT)
            deadline = streams.minute_bar_deadline(datetime.now(UTC)) + timedelta(seconds=60)
            filled = _wait(
                lambda: bool(table_rows(names["latest"])),
                (deadline - datetime.now(UTC)).total_seconds(),
            )
            for line in stream_report(opened[0]):
                evidence(line)
            evidence(f"{found.group(1)}: acknowledged {acked}, rows {table_rows(names['latest'])}")
    finally:
        doc_examples.forget(console.namespaces)
    assert acked, "Massive did not acknowledge the crypto line's topic"
    assert filled, "the crypto line's latest stayed empty"


@pytest.mark.timeout(150)
def test_tester_example_10_fails_the_live_minute_bars_on_basic(
    basic_key: str,
    *,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    console_command: ConsoleCommand,
    table_rows: TableRows,
    capsys: pytest.CaptureFixture[str],
    evidence: Evidence,
) -> None:
    """Whether the tester guide's example 10 fails its live minute bars with Massive's refusal.

    Runs the example as the guide gives it with a Stocks Basic key (`MASSIVE_BASIC_API_KEY`,
    or the main key on Stocks Basic), which no plan setting of a paid key can stand in for,
    and records the three lines it prints. Passes when the `stocks.bars` row of `streams` reads
    `FAILED` with an error that names `AM.AAPL`, the topic Massive refused.
    """
    monkeypatch.setenv("MASSIVE_API_KEY", basic_key)
    code = guide_blocks(TESTER_GUIDE.read_text(encoding="utf-8"))[10]
    console = _Console(tmp_path, console_command, evidence)

    def bars(names: Namespace) -> Rows:
        return [r for r in table_rows(names["streams"]) if r["Dataset"] == "stocks.bars"]

    try:
        with doc_examples.recorded_sessions():
            names = console.run([doc_examples.Block(code, continues=False)], "tester example 10")
            for line in capsys.readouterr().out.splitlines():
                evidence(f"printed: {line}")
            _wait(lambda: any(row["State"] == "FAILED" for row in bars(names)), _REFUSAL_WAIT)
            rows = bars(names)
            for row in table_rows(names["connections"]):
                evidence(f"connection {row['AssetClass']} {row['State']} {row['LastError']!r}")
    finally:
        doc_examples.forget(console.namespaces)
    evidence(f"stocks.bars: {[(row['State'], row['Error']) for row in rows]}")
    assert [row["State"] for row in rows] == ["FAILED"]
    assert "AM.AAPL" in str(rows[0]["Error"])
