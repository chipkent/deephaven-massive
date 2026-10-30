"""The live run's `responses` group: Massive's real replies to the documentation's examples.

Each example is a small piece of user code run through the library's public API, such as NVDA's
daily bars around its June 2024 split. While it runs, the wire package's reply-capture hook
(`set_reply_capture`) hands every REST body and WebSocket frame the library receives to a sink;
the sink puts it on a queue, and a writer thread writes one `CapturedReply` per line to
`<name>.jsonl` in the attempt's folder. The replies are already cleaned of the key by the
clients. Each example runs in its own `MassiveSession`, closed before the next starts, so no
reply of one example lands in another's file, and every `history()` reads REST
(`source=dm.HistorySource.REST`): the group's child runs without the S3 pair, since the hook
does not see flat files.

An example is skipped, with what it needs, when the key's subscriptions lack it
(`skip_reason`): a tier, or history further back than the tier keeps. `index.json` lists every
example with its outcome, its exception if any, and the entitlement it ran under; each result
also goes to the attempt's results file, which the program reads (`live_run`).

An example passes when its expected replies were captured: data (no error, and at least one
reply), a refusal (Massive refused it, which the example exists to record), or any reply.
"""

from __future__ import annotations

import dataclasses
import json
import logging
import queue
import threading
from dataclasses import dataclass
from datetime import date, timedelta
from typing import TYPE_CHECKING, Final

import deephaven_massive as dm
from deephaven_massive._massive import set_reply_capture
from tests._live.calendar import trading_day_before
from tests._live.live_run.stream_capture import diagnostics_rows
from tests._live.plans import TIERS, LivePlans, history_start, plan_text, unmet

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from deephaven.table import Table

    from deephaven_massive import MassiveSession
    from deephaven_massive._massive import CapturedReply

logger = logging.getLogger(__name__)

DATA: Final = "data"
REFUSAL: Final = "refusal"
REPLY: Final = "reply"
"""What an example expects: data, Massive's refusal, or any reply at all."""

STREAM_SECONDS: Final = 180.0
"""How long a stream example is held."""


def hold_for(seconds: float) -> Callable[[MassiveSession], None]:
    """Return a hold that keeps a stream example open for a number of seconds.

    Args:
        seconds: How long.

    Returns:
        A function of the example's session that waits that long.
    """

    def hold(_session: MassiveSession) -> None:
        threading.Event().wait(seconds)

    return hold


_REFUSAL_MARKS: Final = ("HTTP 403", "NOT_AUTHORIZED", "not entitled", "plan may not include")
"""Text that marks a failed stream's error as a refusal by Massive."""


@dataclass(frozen=True)
class Example:
    """One documentation example: user code whose replies are recorded.

    Attributes:
        name: The file name of its replies, such as `"nvda-daily-2024-06-adjusted"`.
        run: Runs it on a session and the run's New York date; returns its table, or None.
        needs: The subscriptions it needs, in the live tests' `needs` grammar.
        history: The class and the earliest day it reads, when it reads a fixed past day.
        expects: `DATA`, `REFUSAL`, or `REPLY`.
        stream: Whether it is a stream, held for `STREAM_SECONDS` (left out by `--any-time`).
        only_tier: A class and tier the key must hold exactly: the examples that record a
            refusal of the next tier up.
    """

    name: str
    run: Callable[[MassiveSession, date], Table | None]
    needs: tuple[str, ...] = ()
    history: tuple[str, date] | None = None
    expects: str = DATA
    stream: bool = False
    only_tier: tuple[str, str] | None = None


_COMPANIES: Final = ["AAPL", "MSFT", "NVDA", "JPM", "XOM"]
_REST = dm.HistorySource.REST


def _last_year(today: date) -> tuple[date, date]:
    """Return the year of days before today."""
    return today - timedelta(days=365), today - timedelta(days=1)


EXAMPLES: Final = (
    Example(
        "nvda-daily-2024-06-adjusted",
        lambda m, _: m.stocks.bars("NVDA", dm.Timespan.DAY).history(
            "2024-06-03", "2024-06-14", source=_REST
        ),
        history=("stocks", date(2024, 6, 3)),
    ),
    Example(
        "nvda-daily-2024-06-as-traded",
        lambda m, _: m.stocks.bars("NVDA", dm.Timespan.DAY, adjusted=False).history(
            "2024-06-03", "2024-06-14", source=_REST
        ),
        history=("stocks", date(2024, 6, 3)),
    ),
    Example("nvda-splits", lambda m, _: m.stocks.splits("NVDA").fetch()),
    Example("market-status", lambda m, _: m.reference.market_status().fetch()),
    Example("market-holidays", lambda m, _: m.reference.market_holidays().fetch()),
    Example(
        "ticker-details", lambda m, _: m.reference.ticker_details(["AAPL", "MSFT", "NVDA"]).fetch()
    ),
    Example(
        "daily-bars-last-year",
        lambda m, today: m.stocks.bars(_COMPANIES, dm.Timespan.DAY).history(
            *_last_year(today), source=_REST
        ),
    ),
    Example(
        "dividends-last-year",
        lambda m, today: m.stocks.dividends(
            _COMPANIES, start=_last_year(today)[0], end=_last_year(today)[1]
        ).fetch(),
    ),
    Example(
        "nvda-minute-bars-last-trading-day",
        lambda m, today: m.stocks.bars("NVDA", dm.Timespan.MINUTE).history(
            trading_day_before(today), trading_day_before(today), source=_REST
        ),
    ),
    Example("nvda-previous-day-bar", lambda m, _: m.stocks.previous_day_bar("NVDA").fetch()),
    Example(
        "treasury-yields-last-month",
        lambda m, today: m.economy.treasury_yields(start=today - timedelta(days=31)).fetch(),
    ),
    Example(
        "unknown-ticker-details",
        lambda m, _: m.reference.ticker_details("ZZZZZQ").fetch(),
        expects=REPLY,
    ),
    Example(
        "refusal-stocks-starter",
        lambda m, _: m.stocks.snapshots("AAPL").fetch(),
        expects=REFUSAL,
        only_tier=("stocks", "basic"),
    ),
    Example(
        "refusal-stocks-developer",
        lambda m, _: m.stocks.last_trade("AAPL").fetch(),
        expects=REFUSAL,
        only_tier=("stocks", "starter"),
    ),
    Example(
        "refusal-stocks-advanced",
        lambda m, _: m.stocks.last_quote("AAPL").fetch(),
        expects=REFUSAL,
        only_tier=("stocks", "developer"),
    ),
    Example(
        "refusal-options-starter",
        lambda m, _: m.options.snapshots("O:SPY261218C00600000").fetch(),
        expects=REFUSAL,
        only_tier=("options", "basic"),
    ),
    Example(
        "refusal-indices-starter",
        lambda m, _: m.indices.snapshots("I:SPX").fetch(),
        expects=REFUSAL,
        only_tier=("indices", "basic"),
    ),
    Example(
        "refusal-currencies-starter",
        lambda m, _: m.forex.last_quote("C:EURUSD").fetch(),
        expects=REFUSAL,
        only_tier=("currencies", "basic"),
    ),
    Example(
        "refusal-currencies-business",
        lambda m, _: m.crypto.fmv("X:BTCUSD").append_only(),
        expects=REFUSAL,
        stream=True,
        only_tier=("currencies", "starter"),
    ),
    Example(
        "refusal-futures-starter",
        lambda m, _: m.futures.snapshots(dm.ALL, product_code="ES", limit=1).fetch(),
        expects=REFUSAL,
        only_tier=("futures", "basic"),
    ),
    Example(
        "stream-stock-minute-bars",
        lambda m, _: m.stocks.bars(["AAPL", "NVDA"], dm.Timespan.MINUTE).append_only(),
        needs=("stocks:starter",),
        stream=True,
    ),
    Example(
        "stream-crypto-minute-bars",
        lambda m, _: m.crypto.bars("X:BTCUSD", dm.Timespan.MINUTE).append_only(),
        needs=("currencies:starter",),
        stream=True,
    ),
    Example(
        "stream-forex-quotes",
        lambda m, _: m.forex.quotes("C:EURUSD").append_only(),
        needs=("currencies:starter",),
        stream=True,
    ),
)
"""Every example, in the order they run."""


def history_need(name: str, day: date, today: date) -> str:
    """Name the cheapest tier of a class whose history reaches a day.

    Args:
        name: The class.
        day: The earliest day an example reads.
        today: The run's New York date.

    Returns:
        Such as `"needs Stocks Starter ($29/month) (history back to 2024-06-03)"`; the last
        tier of every class keeps all history, so one always reaches it.
    """
    tier = next(t for t in TIERS[name][1:] if _keeps(name, t, day=day, today=today))
    return f"needs {plan_text(f'{name}:{tier}')} (history back to {day.isoformat()})"


def _keeps(name: str, tier: str, *, day: date, today: date) -> bool:
    """Tell whether a tier of a class keeps a day, by the documented history windows.

    Args:
        name: The class.
        tier: The tier.
        day: The day.
        today: The run's New York date.

    Returns:
        True when the day is inside the tier's window.
    """
    first = history_start(LivePlans({name: tier}, s3=False, flat_files=(), source=""), name, today)
    return first is None or day >= first


def skip_reason(example: Example, plans: LivePlans, today: date) -> str | None:
    """Decide whether the key can run an example.

    Args:
        example: The example.
        plans: The key's tiers.
        today: The run's New York date.

    Returns:
        Why it is skipped (starting `needs`, or `not this tier` for a refusal example of
        another tier), or None when it runs.
    """
    if example.only_tier is not None:
        name, tier = example.only_tier
        if plans.tiers[name] != tier:
            return f"not this tier: records the refusal on {name} {tier}"
    missing = unmet(example.needs, plans)
    if missing:
        return "needs " + "; ".join(plan_text(r) for r in missing)
    if example.history is not None:
        name, day = example.history
        if not _keeps(name, plans.tiers[name], day=day, today=today):
            return history_need(name, day, today)
    return None


class _Writer:
    """A thread that writes captured replies to one file, fed by the capture sink.

    The sink runs on the library's receiving threads and must not block, so it only puts each
    reply on a queue; this thread writes them, one JSON line each, until `close`.
    """

    def __init__(self, path: Path) -> None:
        """Open the file and start the thread.

        Args:
            path: The example's replies file.
        """
        self.count = 0
        self._queue: queue.SimpleQueue[CapturedReply | None] = queue.SimpleQueue()
        self._file = path.open("a", encoding="utf-8")
        self._thread = threading.Thread(target=self._drain, name=f"live run replies {path.stem}")
        self._thread.start()

    def put(self, reply: CapturedReply) -> None:
        """Queue one reply; called by the sink on any thread.

        Args:
            reply: The reply.
        """
        self._queue.put(reply)

    def _drain(self) -> None:
        """Write queued replies until the end marker."""
        while (reply := self._queue.get()) is not None:
            self._file.write(json.dumps(dataclasses.asdict(reply)) + "\n")
            self._file.flush()
            self.count += 1

    def close(self) -> int:
        """Write what is queued, stop the thread, and close the file.

        Returns:
            How many replies were written.
        """
        self._queue.put(None)
        self._thread.join()
        self._file.close()
        return self.count


def run_example(
    example: Example,
    *,
    open_session: Callable[[], MassiveSession],
    folder: Path,
    today: date,
    hold: Callable[[MassiveSession], None],
) -> tuple[str, str]:
    """Run one example in its own session, recording every reply it draws.

    Args:
        example: The example.
        open_session: Opens a fresh session on the key.
        folder: The attempt's folder; the replies go to `<name>.jsonl` there.
        today: The run's New York date.
        hold: Keeps a stream example open, given its session (`hold_for`).

    Returns:
        `(status, reason)`: `passed`, or `failed` with what was missing or what was raised.
    """
    writer = _Writer(folder / f"{example.name}.jsonl")
    set_reply_capture(writer.put)
    error: BaseException | None = None
    stream_error = ""
    session = open_session()
    try:
        table = example.run(session, today)
        if example.stream and table is not None:
            hold(session)
            failed = [
                r for r in diagnostics_rows(session, session.streams()) if r["State"] == "FAILED"
            ]
            stream_error = str(failed[0]["Error"]) if failed else ""
    except Exception as raised:
        logger.exception("example %s raised", example.name)  # its traceback, in group.log
        error = raised
    finally:
        session.close()
        set_reply_capture(None)
        count = writer.close()
    return _verdict(example, error=error, stream_error=stream_error, count=count)


def _verdict(
    example: Example, *, error: BaseException | None, stream_error: str, count: int
) -> tuple[str, str]:
    """Judge an example's outcome against what it expects.

    Args:
        example: The example.
        error: What it raised, or None.
        stream_error: A failed stream's error, or `""`.
        count: How many replies were captured.

    Returns:
        `(status, reason)`.
    """
    if count == 0:
        raised = f": {type(error).__name__}: {error}" if error is not None else ""
        return "failed", f"no reply was captured{raised}"
    refused = isinstance(error, dm.EntitlementError) or any(
        mark in stream_error for mark in _REFUSAL_MARKS
    )
    if example.expects == REFUSAL:
        return ("passed", "") if refused else ("failed", "expected a refusal")
    if example.expects == DATA and (error is not None or stream_error):
        shown = f"{type(error).__name__}: {error}" if error is not None else stream_error
        return "failed", shown
    return "passed", ""


def run_responses(
    examples: list[Example],
    *,
    plans: LivePlans,
    open_session: Callable[[], MassiveSession],
    folder: Path,
    today: date,
    entitlement: str,
    report: Callable[[str, str, str], None],
    hold: Callable[[MassiveSession], None] | None = None,
) -> None:
    """Run the due examples, one session each, and write `index.json`.

    Args:
        examples: The due examples, in order.
        plans: The key's tiers.
        open_session: Opens a fresh session on the key.
        folder: The attempt's folder.
        today: The run's New York date.
        entitlement: The entitlement id, recorded in the index.
        report: Receives each example's name, status, and reason as it finishes.
        hold: Keeps each stream example open; `hold_for(STREAM_SECONDS)` by default.
    """
    hold = hold or hold_for(STREAM_SECONDS)
    index = []
    for example in examples:
        reason = skip_reason(example, plans, today)
        if reason is None:
            status, reason = run_example(
                example,
                open_session=open_session,
                folder=folder,
                today=today,
                hold=hold,
            )
        else:
            status = "skipped"
        report(example.name, status, reason)
        index.append(
            {"name": example.name, "status": status, "reason": reason, "entitlement": entitlement}
        )
        (folder / "index.json").write_text(json.dumps(index, indent=1) + "\n", encoding="utf-8")
