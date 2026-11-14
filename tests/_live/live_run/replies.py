"""The live run's `responses` group: Massive's real replies to the documentation's examples.

Each example (`reply_examples.EXAMPLES`) is a small piece of user code run through the library's
public API, such as NVDA's daily bars around its June 2024 split; its code is here, in `CALLS`,
by the example's name. While it runs, the wire package's reply-capture hook
(`set_reply_capture`) hands every REST body and WebSocket frame the library receives to a sink;
the sink puts it on a queue, and a writer thread writes one `CapturedReply` per line to
`<name>.jsonl` in the attempt's folder. The replies are already cleaned of the key by the
clients. Each example runs in its own `MassiveSession`, closed before the next starts, so no
reply of one example lands in another's file, and every `history()` reads REST
(`source=dm.HistorySource.REST`): the group's child runs without the S3 pair, since the hook
does not see flat files.

An example the key's subscriptions lack is skipped (`reply_examples.skip_reason`). `index.json`
lists every example with its outcome, its exception if any, and the entitlement it ran under;
each result also goes to the attempt's results file, which the program reads (`live_run`).

An example passes when its expected replies were captured: data (no error, and at least one
reply), a refusal (Massive refused it, which the example exists to record), or any reply.
"""

from __future__ import annotations

import dataclasses
import json
import logging
import queue
import threading
from datetime import date, timedelta
from typing import TYPE_CHECKING, Final

import deephaven_massive as dm
from deephaven_massive._massive import set_reply_capture
from tests._live.calendar import trading_day_before
from tests._live.live_run.reply_examples import DATA, REFUSAL, Example, skip_reason
from tests._live.live_run.stream_capture import diagnostics_rows

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from deephaven.table import Table

    from deephaven_massive import MassiveSession
    from deephaven_massive._massive import CapturedReply
    from tests._live.plans import LivePlans

logger = logging.getLogger(__name__)

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


_REFUSAL_MARKS: Final = (
    "HTTP 403",
    "NOT_AUTHORIZED",
    "not entitled",
    "may not include this data",
    "needs a Business plan",
)
"""Text that marks a failed stream's error as a refusal by Massive: an HTTP 403, a
`NOT_AUTHORIZED` envelope, and the library's wording of a refused topic, including a
fair-market-value (FMV) topic, which only a Business plan serves."""


_COMPANIES: Final = ["AAPL", "MSFT", "NVDA", "JPM", "XOM"]
_REST = dm.HistorySource.REST


def _last_year(today: date) -> tuple[date, date]:
    """Return the year of days before today."""
    return today - timedelta(days=365), today - timedelta(days=1)


CALLS: Final[dict[str, Callable[[MassiveSession, date], Table | None]]] = {
    "nvda-daily-2024-06-adjusted": lambda m, _: m.stocks.bars("NVDA", dm.Timespan.DAY).history(
        "2024-06-03", "2024-06-14", source=_REST, wait=True
    ),
    "nvda-daily-2024-06-as-traded": lambda m, _: m.stocks.bars(
        "NVDA", dm.Timespan.DAY, adjusted=False
    ).history("2024-06-03", "2024-06-14", source=_REST, wait=True),
    "nvda-splits": lambda m, _: m.stocks.splits("NVDA").fetch(wait=True),
    "market-status": lambda m, _: m.reference.market_status().fetch(wait=True),
    "market-holidays": lambda m, _: m.reference.market_holidays().fetch(wait=True),
    "ticker-details": lambda m, _: m.reference.ticker_details(["AAPL", "MSFT", "NVDA"]).fetch(
        wait=True
    ),
    "daily-bars-last-year": lambda m, today: m.stocks.bars(_COMPANIES, dm.Timespan.DAY).history(
        *_last_year(today), source=_REST, wait=True
    ),
    "dividends-last-year": lambda m, today: m.stocks.dividends(
        _COMPANIES, start=_last_year(today)[0], end=_last_year(today)[1]
    ).fetch(wait=True),
    "nvda-minute-bars-last-trading-day": lambda m, today: m.stocks.bars(
        "NVDA", dm.Timespan.MINUTE
    ).history(trading_day_before(today), trading_day_before(today), source=_REST, wait=True),
    "nvda-previous-day-bar": lambda m, _: m.stocks.previous_day_bar("NVDA").fetch(wait=True),
    "treasury-yields-last-month": lambda m, today: m.economy.treasury_yields(
        start=today - timedelta(days=31)
    ).fetch(wait=True),
    "unknown-ticker-details": lambda m, _: m.reference.ticker_details("ZZZZZQ").fetch(wait=True),
    "refusal-stocks-starter": lambda m, _: m.stocks.snapshots("AAPL").fetch(wait=True),
    "refusal-stocks-developer": lambda m, _: m.stocks.last_trade("AAPL").fetch(wait=True),
    "refusal-stocks-advanced": lambda m, _: m.stocks.last_quote("AAPL").fetch(wait=True),
    "refusal-options-starter": lambda m, _: m.options.snapshots("O:SPY261218C00600000").fetch(
        wait=True
    ),
    "refusal-indices-starter": lambda m, _: m.indices.snapshots("I:SPX").fetch(wait=True),
    "refusal-currencies-starter": lambda m, _: m.forex.last_quote("C:EURUSD").fetch(wait=True),
    "refusal-currencies-business": lambda m, _: m.crypto.fmv("X:BTCUSD").append_only(),
    "refusal-futures-starter": lambda m, _: m.futures.snapshots(
        dm.ALL, product_code="ES", limit=1
    ).fetch(wait=True),
    "stream-stock-minute-bars": lambda m, _: m.stocks.bars(
        ["AAPL", "NVDA"], dm.Timespan.MINUTE
    ).append_only(),
    "stream-crypto-minute-bars": lambda m, _: m.crypto.bars(
        "X:BTCUSD", dm.Timespan.MINUTE
    ).append_only(),
    "stream-forex-quotes": lambda m, _: m.forex.quotes("C:EURUSD").append_only(),
}
"""Each example's code, by its name in `reply_examples.EXAMPLES`: it runs on a session and
the run's New York date, and returns its table, or None."""


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
    call: Callable[[MassiveSession, date], Table | None],
    *,
    open_session: Callable[[], MassiveSession],
    folder: Path,
    today: date,
    hold: Callable[[MassiveSession], None],
) -> tuple[str, str]:
    """Run one example in its own session, recording every reply it draws.

    Args:
        example: The example.
        call: Its code (`CALLS`): runs it on a session and the run's New York date.
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
        table = call(session, today)
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
                CALLS[example.name],
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
